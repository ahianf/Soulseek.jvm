// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import dev.slsk.CancellationSignal;
import dev.slsk.CancellationSubscription;
import dev.slsk.common.EventDispatch;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.ConnectionReadException;
import dev.slsk.exceptions.ConnectionWriteDroppedException;
import dev.slsk.exceptions.ConnectionWriteException;
import dev.slsk.options.ConnectionOptions;
import dev.slsk.options.ProxyOptions;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Provides client connections for TCP network services. */
public class SocketConnection implements Connection {
    private static final ExecutorService IO_EXECUTOR =
            Executors.newCachedThreadPool(daemonFactory("soulseek-connection-io"));
    private static final ScheduledExecutorService TIMER_EXECUTOR =
            Executors.newScheduledThreadPool(2, daemonFactory("soulseek-connection-timer"));

    private final UUID id = UUID.randomUUID();
    private final CopyOnWriteArrayList<ConnectionEventListener<Void>> connectedListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<ConnectionEventListener<ConnectionDataEvent>> dataReadListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<ConnectionEventListener<ConnectionDataEvent>> dataWrittenListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<ConnectionEventListener<ConnectionDisconnectedEvent>> disconnectedListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<ConnectionEventListener<ConnectionStateChangedEvent>> stateChangedListeners =
            new CopyOnWriteArrayList<>();
    private final CompletableFuture<String> disconnectFuture = new CompletableFuture<>();
    private final Semaphore writeSemaphore = new Semaphore(1);
    private final Semaphore writeQueueSemaphore;
    private final Object timerLock = new Object();

    private volatile boolean disposed;
    private volatile long lastActivityNanos = System.nanoTime();
    private volatile ConnectionState state = ConnectionState.PENDING;
    private volatile ConnectionTypes type = ConnectionTypes.NONE;
    private volatile boolean writeQueueFull;
    private ScheduledFuture<?> inactivityTask;
    private ScheduledFuture<?> watchdogTask;

    protected InetSocketAddress ipEndpoint;
    protected final ConnectionOptions options;
    protected volatile NetworkStream stream;
    protected volatile TcpClient tcpClient;

    /** Creates a connection with source defaults. */
    public SocketConnection(InetSocketAddress ipEndpoint) {
        this(ipEndpoint, null, null);
    }

    /** Creates a connection with the supplied options. */
    public SocketConnection(InetSocketAddress ipEndpoint, ConnectionOptions options) {
        this(ipEndpoint, options, null);
    }

    /** Creates a connection over an optional existing TCP client. */
    public SocketConnection(InetSocketAddress ipEndpoint, ConnectionOptions options, TcpClient tcpClient) {
        this.ipEndpoint = ipEndpoint;
        this.options = options == null ? new ConnectionOptions() : options;
        this.tcpClient = tcpClient == null ? new TcpClientAdapter() : tcpClient;
        writeQueueSemaphore = new Semaphore(this.options.getWriteQueueSize());

        try {
            this.options.getConfigureSocket().configure(this.tcpClient.getClient());
            setSocketTimeout(this.options.getInactivityTimeout());

            if (this.tcpClient.isConnected()) {
                state = ConnectionState.CONNECTED;
                startTimers();
                stream = this.tcpClient.getStream();
                setStreamTimeouts();
            }
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    @Override
    public void addConnectedListener(ConnectionEventListener<Void> listener) {
        connectedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeConnectedListener(ConnectionEventListener<Void> listener) {
        connectedListeners.remove(listener);
    }

    @Override
    public void addDataReadListener(ConnectionEventListener<ConnectionDataEvent> listener) {
        dataReadListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeDataReadListener(ConnectionEventListener<ConnectionDataEvent> listener) {
        dataReadListeners.remove(listener);
    }

    @Override
    public void addDataWrittenListener(ConnectionEventListener<ConnectionDataEvent> listener) {
        dataWrittenListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeDataWrittenListener(ConnectionEventListener<ConnectionDataEvent> listener) {
        dataWrittenListeners.remove(listener);
    }

    @Override
    public void addDisconnectedListener(ConnectionEventListener<ConnectionDisconnectedEvent> listener) {
        disconnectedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeDisconnectedListener(ConnectionEventListener<ConnectionDisconnectedEvent> listener) {
        disconnectedListeners.remove(listener);
    }

    @Override
    public void addStateChangedListener(ConnectionEventListener<ConnectionStateChangedEvent> listener) {
        stateChangedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeStateChangedListener(ConnectionEventListener<ConnectionStateChangedEvent> listener) {
        stateChangedListeners.remove(listener);
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public Duration getInactiveTime() {
        return Duration.ofNanos(Math.max(0, System.nanoTime() - lastActivityNanos));
    }

    @Override
    public InetSocketAddress getIpEndpoint() {
        return ipEndpoint;
    }

    @Override
    public ConnectionKey getKey() {
        return new ConnectionKey(ipEndpoint);
    }

    @Override
    public ConnectionOptions getOptions() {
        return options;
    }

    @Override
    public ConnectionState getState() {
        return state;
    }

    @Override
    public ConnectionTypes getType() {
        return type;
    }

    @Override
    public void setType(ConnectionTypes type) {
        this.type = type;
    }

    @Override
    public int getWriteQueueDepth() {
        return options.getWriteQueueSize() - writeQueueSemaphore.availablePermits();
    }

    @Override
    public CompletableFuture<Void> connectAsync(CancellationSignal cancellationSignal) {
        if (state != ConnectionState.PENDING && state != ConnectionState.DISCONNECTED) {
            throw new IllegalStateException("Invalid attempt to connect a connected or "
                    + "transitioning connection (current state: "
                    + state + ")");
        }
        CancellationSignal token = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;

        changeState(ConnectionState.CONNECTING, "Connecting to " + formatEndpoint(ipEndpoint), null);

        CompletableFuture<?> connectTask;
        try {
            ProxyOptions proxy = options.getProxyOptions();
            if (proxy != null) {
                connectTask = tcpClient.connectThroughProxyAsync(
                        proxy.getIpEndpoint().getAddress(),
                        proxy.getIpEndpoint().getPort(),
                        ipEndpoint.getAddress(),
                        ipEndpoint.getPort(),
                        proxy.getUsername(),
                        proxy.getPassword(),
                        token);
            } else {
                connectTask = tcpClient.connectAsync(ipEndpoint.getAddress(), ipEndpoint.getPort());
            }
        } catch (Exception exception) {
            return connectFailureFuture(exception);
        }

        CompletableFuture<Void> gate = new CompletableFuture<>();
        ScheduledFuture<?> timeoutTask;
        try {
            int timeout = options.getConnectTimeout();
            if (timeout < -1) {
                throw new IllegalArgumentException("Connect timeout must be -1 or non-negative");
            }
            timeoutTask = timeout == -1
                    ? null
                    : TIMER_EXECUTOR.schedule(
                            () -> gate.completeExceptionally(
                                    new TimeoutException("Operation timed out after " + timeout + " milliseconds")),
                            timeout,
                            TimeUnit.MILLISECONDS);
        } catch (Exception exception) {
            return connectFailureFuture(exception);
        }

        CancellationSubscription registration =
                token.register(() -> gate.completeExceptionally(new CancellationException("Operation cancelled")));
        connectTask.whenComplete((ignored, exception) -> {
            if (exception == null) {
                gate.complete(null);
            } else {
                gate.completeExceptionally(unwrap(exception));
            }
        });

        return async(() -> {
            try {
                await(gate);
                startTimers();
                stream = tcpClient.getStream();
                setStreamTimeouts();
                changeState(ConnectionState.CONNECTED, "Connected to " + formatEndpoint(ipEndpoint), null);
                return null;
            } catch (Exception exception) {
                throw handleConnectFailure(exception);
            } finally {
                registration.close();
                if (timeoutTask != null) {
                    timeoutTask.cancel(false);
                }
            }
        });
    }

    @Override
    public void disconnect(String message, Exception exception) {
        synchronized (this) {
            if (state == ConnectionState.DISCONNECTED || state == ConnectionState.DISCONNECTING) {
                return;
            }
            String reason = message != null ? message : exception == null ? null : exception.getMessage();

            changeState(ConnectionState.DISCONNECTING, reason, null);
            stopTimers();
            closeTransport();
            changeState(ConnectionState.DISCONNECTED, reason, exception);
        }
    }

    @Override
    public TcpClient handoffTcpClient() {
        TcpClient result = tcpClient;
        tcpClient = null;
        stream = null;
        return result;
    }

    @Override
    public CompletableFuture<byte[]> readAsync(long length, CancellationSignal cancellationSignal) {
        validateRead(length);
        CancellationSignal token = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
        return async(() -> {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            readInternal(length, output, SocketConnection::grantAll, null, token);
            return output.toByteArray();
        });
    }

    @Override
    public CompletableFuture<Void> readAsync(
            long length,
            OutputStream outputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            CancellationSignal cancellationSignal) {
        if (length < 0) {
            throw new IllegalArgumentException("The requested length must be greater than or equal " + "to zero");
        }
        Objects.requireNonNull(outputStream, "The specified output stream is null");
        validateConnected();
        CancellationSignal token = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
        ConnectionGovernor effectiveGovernor = governor == null ? SocketConnection::grantAll : governor;
        return async(() -> {
            readInternal(length, outputStream, effectiveGovernor, reporter, token);
            return null;
        });
    }

    @Override
    public CompletableFuture<String> waitForDisconnect(CancellationSignal cancellationSignal) {
        if (cancellationSignal != null) {
            cancellationSignal.register(() -> disconnect(null, new CancellationException("Operation cancelled")));
        }
        return disconnectFuture;
    }

    @Override
    public CompletableFuture<Void> writeAsync(byte[] bytes, CancellationSignal cancellationSignal) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("Invalid attempt to send empty data");
        }
        validateConnected();
        CancellationSignal token = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
        return async(() -> {
            writeInternal(
                    bytes.length, new java.io.ByteArrayInputStream(bytes), SocketConnection::grantAll, null, token);
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> writeAsync(
            long length,
            InputStream inputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            CancellationSignal cancellationSignal) {
        if (length <= 0) {
            throw new IllegalArgumentException("The requested length must be greater than or equal " + "to zero");
        }
        Objects.requireNonNull(inputStream, "The specified output stream is null");
        validateConnected();
        CancellationSignal token = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
        ConnectionGovernor effectiveGovernor = governor == null ? SocketConnection::grantAll : governor;
        return async(() -> {
            writeInternal(length, inputStream, effectiveGovernor, reporter, token);
            return null;
        });
    }

    @Override
    public void close() {
        synchronized (this) {
            if (disposed) {
                return;
            }
            disconnect(
                    "SocketConnection is being disposed",
                    new IllegalStateException(getClass().getSimpleName() + " has been disposed"));
            stopTimers();
            closeTransport();
            disposed = true;
        }
    }

    /** Changes state and raises the matching source events. */
    protected void changeState(ConnectionState newState, String message, Exception exception) {
        ConnectionState previousState = state;
        ConnectionStateChangedEvent eventData =
                new ConnectionStateChangedEvent(previousState, newState, message, exception);
        state = newState;

        for (ConnectionEventListener<ConnectionStateChangedEvent> listener : stateChangedListeners) {
            listener.handle(this, eventData);
        }
        if (newState == ConnectionState.CONNECTED) {
            for (ConnectionEventListener<Void> listener : connectedListeners) {
                listener.handle(this, null);
            }
        } else if (newState == ConnectionState.DISCONNECTED) {
            ConnectionDisconnectedEvent disconnected = new ConnectionDisconnectedEvent(message, exception);
            for (ConnectionEventListener<ConnectionDisconnectedEvent> listener : disconnectedListeners) {
                listener.handle(this, disconnected);
            }
            if (exception == null) {
                disconnectFuture.complete(message);
            } else {
                disconnectFuture.completeExceptionally(exception);
            }
        }
    }

    /** Resets the activity timestamp and inactivity timer. */
    protected final void resetInactivityTime() {
        lastActivityNanos = System.nanoTime();
        if (options.getInactivityTimeout() > 0) {
            scheduleInactivityTimeout();
        }
    }

    /** Returns the currently associated network stream. */
    protected final NetworkStream getStream() {
        return stream;
    }

    /** Returns the currently associated TCP client. */
    protected final TcpClient getTcpClient() {
        return tcpClient;
    }

    /** Returns whether this connection has been disposed. */
    protected final boolean isDisposed() {
        return disposed;
    }

    /** Sets state for derived source ports that adopt a connection. */
    protected final void setState(ConnectionState value) {
        state = value;
    }

    private void readInternal(
            long length,
            OutputStream outputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            CancellationSignal cancellationSignal)
            throws Exception {
        resetInactivityTime();
        byte[] buffer = new byte[options.getReadBufferSize()];
        long totalBytesRead = 0;

        try {
            while (!disposed && totalBytesRead < length) {
                cancellationSignal.throwIfCancellationRequested();
                long bytesRemaining = length - totalBytesRead;
                int bytesToRead = bytesRemaining >= buffer.length ? buffer.length : (int) bytesRemaining;
                int bytesGranted = Math.min(bytesToRead, await(governor.grantAsync(bytesToRead, cancellationSignal)));
                int bytesRead = await(stream.readAsync(buffer, 0, bytesGranted, cancellationSignal));
                if (bytesRead == 0) {
                    throw new ConnectionException("Remote connection closed");
                }
                cancellationSignal.throwIfCancellationRequested();
                outputStream.write(buffer, 0, bytesRead);
                totalBytesRead += bytesRead;
                if (reporter != null) {
                    reporter.report(bytesToRead, bytesGranted, bytesRead);
                }
                emitProgress(dataReadListeners, totalBytesRead, length, cancellationSignal);
                resetInactivityTime();
            }
            cancellationSignal.throwIfCancellationRequested();
            outputStream.flush();
        } catch (Exception exception) {
            Exception actual = asException(unwrap(exception));
            disconnect("Read error: " + actual.getMessage(), actual);
            if (actual instanceof TimeoutException || actual instanceof CancellationException) {
                throw actual;
            }
            throw new ConnectionReadException(
                    "Failed to read " + length + " bytes from "
                            + formatEndpoint(ipEndpoint) + ": "
                            + actual.getMessage(),
                    actual);
        }
    }

    private void writeInternal(
            long length,
            InputStream inputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            CancellationSignal cancellationSignal)
            throws Exception {
        if (writeQueueFull || !writeQueueSemaphore.tryAcquire()) {
            writeQueueFull = true;
            disconnect("The write buffer is full", null);
            throw new ConnectionWriteDroppedException(
                    "Dropped buffered message to " + formatEndpoint(ipEndpoint) + "; the write buffer is full");
        }
        acquire(writeSemaphore, cancellationSignal);

        try {
            resetInactivityTime();
            byte[] buffer = new byte[options.getWriteBufferSize()];
            long totalBytesWritten = 0;

            while (totalBytesWritten < length) {
                cancellationSignal.throwIfCancellationRequested();
                if (disposed || state == ConnectionState.DISCONNECTING || state == ConnectionState.DISCONNECTED) {
                    throw new ConnectionWriteException("Write aborted after " + totalBytesWritten
                            + " bytes written; the connection has "
                            + "been or is being "
                            + (disposed ? "disposed" : "disconnected"));
                }
                long bytesRemaining = length - totalBytesWritten;
                int bytesToRead = bytesRemaining >= buffer.length ? buffer.length : (int) bytesRemaining;
                int bytesGranted = Math.min(bytesToRead, await(governor.grantAsync(bytesToRead, cancellationSignal)));
                int bytesRead = inputStream.read(buffer, 0, bytesGranted);
                if (bytesRead < 0) {
                    bytesRead = 0;
                }
                await(stream.writeAsync(buffer, 0, bytesRead, cancellationSignal));
                totalBytesWritten += bytesRead;
                if (reporter != null) {
                    reporter.report(bytesToRead, bytesGranted, bytesRead);
                }
                emitProgress(dataWrittenListeners, totalBytesWritten, length, cancellationSignal);
                resetInactivityTime();
            }
        } catch (Exception exception) {
            Exception actual = asException(unwrap(exception));
            disconnect("Write error: " + actual.getMessage(), actual);
            if (actual instanceof TimeoutException || actual instanceof CancellationException) {
                throw actual;
            }
            throw new ConnectionWriteException(
                    "Failed to write " + length + " bytes to "
                            + formatEndpoint(ipEndpoint) + ": "
                            + actual.getMessage(),
                    actual);
        } finally {
            if (!disposed) {
                writeQueueSemaphore.release();
                writeSemaphore.release();
            }
        }
    }

    private void validateRead(long length) {
        if (length < 0) {
            throw new IllegalArgumentException("The requested length must be greater than or equal " + "to zero");
        }
        validateConnected();
    }

    private void validateConnected() {
        TcpClient client = tcpClient;
        if (client == null || !client.isConnected()) {
            throw new IllegalStateException("The underlying Tcp connection is closed");
        }
        if (state != ConnectionState.CONNECTED) {
            throw new IllegalStateException("Invalid attempt to send to a disconnected or "
                    + "transitioning connection (current state: "
                    + state + ")");
        }
    }

    private void setSocketTimeout(int timeout) throws IOException {
        tcpClient.getClient().setSoTimeout(timeout <= 0 ? 0 : timeout);
    }

    private void setStreamTimeouts() throws IOException {
        stream.setReadTimeout(options.getInactivityTimeout());
        stream.setWriteTimeout(options.getInactivityTimeout());
    }

    private void startTimers() {
        if (options.getInactivityTimeout() > 0) {
            scheduleInactivityTimeout();
        }
        synchronized (timerLock) {
            if (watchdogTask == null || watchdogTask.isDone()) {
                watchdogTask = TIMER_EXECUTOR.scheduleAtFixedRate(this::watchdogTick, 250, 250, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void stopTimers() {
        synchronized (timerLock) {
            if (inactivityTask != null) {
                inactivityTask.cancel(false);
                inactivityTask = null;
            }
            if (watchdogTask != null) {
                watchdogTask.cancel(false);
                watchdogTask = null;
            }
        }
    }

    private void scheduleInactivityTimeout() {
        synchronized (timerLock) {
            if (inactivityTask != null) {
                inactivityTask.cancel(false);
            }
            int timeout = options.getInactivityTimeout();
            inactivityTask = TIMER_EXECUTOR.schedule(
                    () -> {
                        TimeoutException exception =
                                new TimeoutException("Inactivity timeout of " + timeout + " milliseconds was reached");
                        disconnect(exception.getMessage(), exception);
                    },
                    timeout,
                    TimeUnit.MILLISECONDS);
        }
    }

    private void watchdogTick() {
        TcpClient client = tcpClient;
        if (client == null || !client.isConnected()) {
            String message = "The connection was closed unexpectedly";
            disconnect(message, new ConnectionException(message));
        }
    }

    private void closeTransport() {
        NetworkStream currentStream = stream;
        TcpClient currentClient = tcpClient;
        try {
            if (currentStream != null) {
                currentStream.close();
            }
        } catch (IOException ignored) {
            // Source close is best-effort during disconnection.
        }
        try {
            if (currentClient != null) {
                currentClient.close();
            }
        } catch (IOException ignored) {
            // Source close is best-effort during disconnection.
        }
    }

    private CompletableFuture<Void> connectFailureFuture(Exception exception) {
        return failedFuture(handleConnectFailure(exception));
    }

    private Exception handleConnectFailure(Exception exception) {
        Exception actual = asException(unwrap(exception));
        disconnect("SocketConnection Error: " + actual.getMessage(), actual);
        if (actual instanceof TimeoutException || actual instanceof CancellationException) {
            return actual;
        }
        return new ConnectionException(
                "Failed to connect to " + formatEndpoint(ipEndpoint) + ": " + actual.getMessage(), actual);
    }

    private static void acquire(Semaphore semaphore, CancellationSignal cancellationSignal) {
        try {
            while (true) {
                cancellationSignal.throwIfCancellationRequested();
                if (semaphore.tryAcquire(25, TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new CancellationException("Operation cancelled");
        }
    }

    private static CompletableFuture<Integer> grantAll(int requestedBytes, CancellationSignal cancellationSignal) {
        return CompletableFuture.completedFuture(Integer.MAX_VALUE);
    }

    private <T> void emitProgress(
            CopyOnWriteArrayList<ConnectionEventListener<T>> listeners,
            long currentLength,
            long totalLength,
            CancellationSignal cancellationSignal) {
        @SuppressWarnings("unchecked")
        T eventData = (T) new ConnectionDataEvent(currentLength, totalLength);
        Runnable dispatch = () -> {
            for (ConnectionEventListener<T> listener : listeners) {
                listener.handle(this, eventData);
            }
        };
        if (EventDispatch.isAsynchronous()) {
            if (!cancellationSignal.isCancellationRequested()) {
                IO_EXECUTOR.execute(dispatch);
            }
        } else {
            dispatch.run();
        }
    }

    private static <T> CompletableFuture<T> async(Callable<T> callable) {
        CompletableFuture<T> future = new CompletableFuture<>();
        IO_EXECUTOR.execute(() -> {
            try {
                future.complete(callable.call());
            } catch (Throwable exception) {
                future.completeExceptionally(exception);
            }
        });
        return future;
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        try {
            return future.get();
        } catch (ExecutionException exception) {
            Throwable cause = unwrap(exception);
            if (cause instanceof Exception actual) {
                throw actual;
            }
            throw new RuntimeException(cause);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new CancellationException("Operation cancelled");
        }
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable exception) {
        return CompletableFuture.failedFuture(exception);
    }

    private static Throwable unwrap(Throwable exception) {
        Throwable current = exception;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static Exception asException(Throwable throwable) {
        return throwable instanceof Exception exception ? exception : new RuntimeException(throwable);
    }

    private static String formatEndpoint(InetSocketAddress endpoint) {
        if (endpoint == null) {
            return "";
        }
        String address = endpoint.getAddress() == null
                ? endpoint.getHostString()
                : endpoint.getAddress().getHostAddress();
        if (address.contains(":")) {
            address = "[" + address + "]";
        }
        return address + ":" + endpoint.getPort();
    }

    private static ThreadFactory daemonFactory(String prefix) {
        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName(prefix + "-" + thread.threadId());
            thread.setDaemon(true);
            return thread;
        };
    }
}
