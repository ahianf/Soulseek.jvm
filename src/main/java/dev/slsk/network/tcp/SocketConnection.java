// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import dev.slsk.CancellationSignal;
import dev.slsk.CancellationSubscription;
import dev.slsk.common.EventDispatch;
import dev.slsk.common.NetworkExecutor;
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
import java.net.SocketTimeoutException;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Provides client connections for TCP network services. */
public class SocketConnection implements Connection {
    // Blocking socket reads/writes are dispatched on virtual threads so a parked
    // read unmounts its carrier instead of pinning a bounded pool worker; see
    // NetworkExecutor for why the common pool is unusable here.
    private static final ExecutorService IO_EXECUTOR = NetworkExecutor.executor();
    private static final ScheduledExecutorService TIMER_EXECUTOR = createTimerExecutor();

    // One sweep task covers every connection; see ConnectionMonitor. Defect 1.6
    // moves this off a static field so each client owns its own.
    private static final ConnectionMonitor MONITOR = new ConnectionMonitor(TIMER_EXECUTOR);

    /** Fastest monitor tick, so a very short inactivity timeout stays precise. */
    private static final int MIN_MONITOR_INTERVAL_MILLIS = 10;

    /** Slowest monitor tick, matching the original watchdog cadence. */
    private static final int MAX_MONITOR_INTERVAL_MILLIS = 250;

    /**
     * How often a blocked read returns so the loop can check cancellation.
     *
     * <p>Set as {@code SO_TIMEOUT}. A timeout expiry leaves the socket usable
     * and loses no bytes, which is what lets a transfer be cancelled without
     * tearing down the connection carrying it. Interrupting the reader cannot
     * do this: the JDK closes the socket when a virtual thread blocked in
     * {@code Socket} read is interrupted.
     *
     * <p>This is no longer tied to the inactivity timeout. Since the periodic
     * monitor took over inactivity detection, the socket timeout is free to be
     * purely a cancellation poll interval.
     */
    private static final int CANCELLATION_POLL_MILLIS = 250;

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

    private final AtomicBoolean closeStarted = new AtomicBoolean();
    private volatile boolean disposed;
    private volatile long lastActivityNanos = System.nanoTime();
    private volatile ConnectionState state = ConnectionState.PENDING;
    private volatile ConnectionTypes type = ConnectionTypes.NONE;
    private volatile boolean writeQueueFull;

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
            setSocketTimeout(CANCELLATION_POLL_MILLIS);

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

    /**
     * Disconnects, raising the state and disconnected events.
     *
     * <p>The monitor is used only to claim the transition, so exactly one
     * caller proceeds. Everything after that — transport teardown and every
     * listener callback — runs with no library lock held.
     *
     * <p>This previously held {@code synchronized(this)} across both
     * {@code changeState} calls, so every state-changed and disconnected
     * listener ran under the connection's monitor. User code that blocked, or
     * that called back into the connection from another thread, could deadlock
     * against it. On Java 21 it also pinned the carrier thread; JEP 491 removed
     * that half of the problem on 25, but the deadlock exposure was real either
     * way.
     */
    @Override
    public void disconnect(String message, Exception exception) {
        ConnectionState previousState;
        String reason;

        synchronized (this) {
            if (state == ConnectionState.DISCONNECTED || state == ConnectionState.DISCONNECTING) {
                return;
            }
            reason = message != null ? message : exception == null ? null : exception.getMessage();
            previousState = state;
            state = ConnectionState.DISCONNECTING;
        }

        publishStateChanged(previousState, ConnectionState.DISCONNECTING, reason, null);
        stopTimers();
        closeTransport();

        state = ConnectionState.DISCONNECTED;
        publishStateChanged(ConnectionState.DISCONNECTING, ConnectionState.DISCONNECTED, reason, exception);
        publishDisconnected(reason, exception);
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
        return readAsync(length, null, cancellationSignal);
    }

    /**
     * Reads {@code length} bytes, reporting progress to one listener scoped to
     * this read in addition to the registered data-read listeners.
     *
     * <p>Callers that want progress for a single read use this instead of
     * adding and removing themselves from the shared listener list around it.
     * That pattern copied the backing {@link CopyOnWriteArrayList} twice per
     * call, which on the framed read path meant twice per protocol message.
     *
     * @param length the number of bytes to read
     * @param scopedProgress the progress listener for this read, or {@code null}
     * @param cancellationSignal the cancellation signal
     * @return a future containing the bytes read
     */
    protected CompletableFuture<byte[]> readAsync(
            long length,
            ConnectionEventListener<ConnectionDataEvent> scopedProgress,
            CancellationSignal cancellationSignal) {
        validateRead(length);
        CancellationSignal token = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
        return async(() -> {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            readInternal(length, output, SocketConnection::grantAll, null, scopedProgress, token);
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
            readInternal(length, outputStream, effectiveGovernor, reporter, null, token);
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
        // An atomic claim rather than a monitor, because disconnect() below
        // runs user listeners and must not do so under a library lock.
        if (!closeStarted.compareAndSet(false, true)) {
            return;
        }
        disconnect(
                "SocketConnection is being disposed",
                new IllegalStateException(getClass().getSimpleName() + " has been disposed"));
        stopTimers();
        closeTransport();
        disposed = true;
    }

    /**
     * Changes state and raises the matching source events.
     *
     * <p>Callers must not hold a library lock: this invokes user listeners.
     */
    protected void changeState(ConnectionState newState, String message, Exception exception) {
        ConnectionState previousState = state;
        state = newState;

        publishStateChanged(previousState, newState, message, exception);
        if (newState == ConnectionState.CONNECTED) {
            for (ConnectionEventListener<Void> listener : connectedListeners) {
                listener.handle(this, null);
            }
        } else if (newState == ConnectionState.DISCONNECTED) {
            publishDisconnected(message, exception);
        }
    }

    /** Raises the state-changed event. Must be called with no lock held. */
    private void publishStateChanged(
            ConnectionState previousState, ConnectionState newState, String message, Exception exception) {
        ConnectionStateChangedEvent eventData =
                new ConnectionStateChangedEvent(previousState, newState, message, exception);
        for (ConnectionEventListener<ConnectionStateChangedEvent> listener : stateChangedListeners) {
            listener.handle(this, eventData);
        }
    }

    /**
     * Raises the disconnected event and settles the disconnect future. Must be
     * called with no lock held.
     */
    private void publishDisconnected(String message, Exception exception) {
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

    /**
     * Records activity.
     *
     * <p>Called once per buffer chunk on both the read and write paths, so it
     * must stay a single volatile store. It used to cancel and reschedule a
     * {@link ScheduledFuture} on every call; because the shared executor did
     * not evict cancelled tasks, each reschedule left a dead entry in the delay
     * queue for the whole inactivity window. A 2 GiB transfer at the 16 KiB
     * default buffer ended with 131,077 of them — one per chunk. The periodic
     * monitor reads this timestamp instead.
     */
    protected final void resetInactivityTime() {
        lastActivityNanos = System.nanoTime();
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
            ConnectionEventListener<ConnectionDataEvent> scopedProgress,
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

                int bytesRead;
                try {
                    bytesRead = await(stream.readAsync(buffer, 0, bytesGranted, cancellationSignal));
                } catch (SocketTimeoutException timeout) {
                    // No data inside the poll window. This is the cancellation
                    // check point: the socket is untouched and no bytes were
                    // lost, so the loop can simply go round again. Report a
                    // zero transfer first so the caller returns the rate-limit
                    // tokens it granted for an attempt that moved nothing.
                    if (reporter != null) {
                        reporter.report(bytesToRead, bytesGranted, 0);
                    }
                    continue;
                }

                if (bytesRead == 0) {
                    throw new ConnectionException("Remote connection closed");
                }
                cancellationSignal.throwIfCancellationRequested();
                outputStream.write(buffer, 0, bytesRead);
                totalBytesRead += bytesRead;
                if (reporter != null) {
                    reporter.report(bytesToRead, bytesGranted, bytesRead);
                }
                emitProgress(dataReadListeners, scopedProgress, totalBytesRead, length, cancellationSignal);
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
                emitProgress(dataWrittenListeners, null, totalBytesWritten, length, cancellationSignal);
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
        tcpClient.getClient().setSoTimeout(timeout);
    }

    private void setStreamTimeouts() throws IOException {
        // The read timeout is the cancellation poll interval, not the
        // inactivity budget; the periodic monitor owns inactivity now.
        stream.setReadTimeout(CANCELLATION_POLL_MILLIS);
        // SO_TIMEOUT does not apply to writes in Java, so this stays
        // informational; write cancellation is checked between chunks.
        stream.setWriteTimeout(options.getInactivityTimeout());
    }

    private void startTimers() {
        MONITOR.register(this);
    }

    private void stopTimers() {
        MONITOR.unregister(this);
    }

    /**
     * Returns the monitor cadence.
     *
     * <p>Capped at the original 250 ms watchdog interval so liveness detection
     * is unchanged, and scaled down for short inactivity timeouts so those stay
     * about as precise as the old dedicated one-shot timer.
     */
    int monitorIntervalMillis() {
        int timeout = options.getInactivityTimeout();
        if (timeout <= 0) {
            return MAX_MONITOR_INTERVAL_MILLIS;
        }
        return Math.clamp(timeout / 4, MIN_MONITOR_INTERVAL_MILLIS, MAX_MONITOR_INTERVAL_MILLIS);
    }

    /**
     * Both checks the connection needs: that the transport is still there, and
     * that it has not gone idle past its budget.
     *
     * <p>Driven by the shared {@link ConnectionMonitor} sweep, not by a task of
     * this connection's own.
     */
    void monitorTick() {
        TcpClient client = tcpClient;
        if (client == null || !client.isConnected()) {
            String message = "The connection was closed unexpectedly";
            disconnect(message, new ConnectionException(message));
            return;
        }

        int timeout = options.getInactivityTimeout();
        if (timeout > 0) {
            long idleMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastActivityNanos);
            if (idleMillis >= timeout) {
                TimeoutException exception =
                        new TimeoutException("Inactivity timeout of " + timeout + " milliseconds was reached");
                disconnect(exception.getMessage(), exception);
            }
        }
    }

    private static ScheduledExecutorService createTimerExecutor() {
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(2, daemonFactory("soulseek-connection-timer"));
        // Without this, a cancelled task stays resident in the delay queue
        // until its original deadline passes. Defence in depth now that the
        // per-chunk reschedule is gone.
        executor.setRemoveOnCancelPolicy(true);
        return executor;
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

    /**
     * Acquires a permit, blocking until one is available.
     *
     * <p>This used to spin {@code tryAcquire(25, MILLISECONDS)} so that it could
     * notice cancellation between attempts, which is how C#'s natively
     * cancellable {@code SemaphoreSlim.WaitAsync(token)} was emulated. Every
     * queued writer therefore woke forty times a second doing nothing.
     *
     * <p>The caller is a virtual thread, so a genuine blocking acquire costs
     * nothing while parked. Cancellation arrives as an interrupt instead of
     * being polled for.
     */
    private static void acquire(Semaphore semaphore, CancellationSignal cancellationSignal) {
        Thread caller = Thread.currentThread();
        // Registering runs the callback inline if cancellation already
        // happened, so an already-cancelled signal interrupts before the
        // acquire and takes the InterruptedException path immediately.
        CancellationSubscription registration = cancellationSignal.register(caller::interrupt);
        boolean acquired = false;
        try {
            semaphore.acquire();
            acquired = true;
        } catch (InterruptedException exception) {
            throw new CancellationException("Operation cancelled");
        } finally {
            registration.close();
            // A cancellation racing a successful acquire can land the interrupt
            // after the permit is taken. Clear it so it cannot leak into the
            // caller's next blocking call, and give the permit back.
            boolean interrupted = Thread.interrupted();
            if (interrupted && acquired) {
                semaphore.release();
                throw new CancellationException("Operation cancelled");
            }
        }
    }

    private static CompletableFuture<Integer> grantAll(int requestedBytes, CancellationSignal cancellationSignal) {
        return CompletableFuture.completedFuture(Integer.MAX_VALUE);
    }

    /**
     * Raises a progress event to the registered listeners and, optionally, to
     * one caller-supplied listener scoped to a single read.
     *
     * <p>The scoped listener exists so that a caller wanting progress for one
     * operation does not have to add and remove itself from
     * {@code dataReadListeners} around it. That pattern cost two
     * {@link CopyOnWriteArrayList} array copies per protocol message on the
     * framed read path; measured at 14.7x the cost of dispatch alone with no
     * listeners registered.
     */
    private void emitProgress(
            CopyOnWriteArrayList<ConnectionEventListener<ConnectionDataEvent>> listeners,
            ConnectionEventListener<ConnectionDataEvent> scopedListener,
            long currentLength,
            long totalLength,
            CancellationSignal cancellationSignal) {
        if (listeners.isEmpty() && scopedListener == null) {
            return;
        }
        ConnectionDataEvent eventData = new ConnectionDataEvent(currentLength, totalLength);
        Runnable dispatch = () -> {
            for (ConnectionEventListener<ConnectionDataEvent> listener : listeners) {
                listener.handle(this, eventData);
            }
            if (scopedListener != null) {
                scopedListener.handle(this, eventData);
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
