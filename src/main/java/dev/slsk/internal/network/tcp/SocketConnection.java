// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import dev.slsk.Subscription;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.ConnectionReadException;
import dev.slsk.exceptions.ConnectionWriteDroppedException;
import dev.slsk.exceptions.ConnectionWriteException;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.concurrent.CancellationInterrupts;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.concurrent.CancellationSubscription;
import dev.slsk.internal.concurrent.InterruptedOperationException;
import dev.slsk.internal.events.Subscriptions;
import dev.slsk.internal.options.ConnectionOptions;
import dev.slsk.internal.options.ProxyOptions;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Provides client connections for TCP network services. */
public class SocketConnection implements Connection {

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

    /** {@link NetworkStream}'s sentinel for "block until data arrives". */
    private static final int NO_READ_TIMEOUT = -1;

    private final UUID id = UUID.randomUUID();
    private final CopyOnWriteArrayList<Consumer<? super Connection>> connectedListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super ConnectionDataEvent>> dataReadListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super ConnectionDataEvent>> dataWrittenListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super ConnectionDisconnectedEvent>> disconnectedListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super ConnectionStateChangedEvent>> stateChangedListeners =
            new CopyOnWriteArrayList<>();
    private final CountDownLatch disconnected = new CountDownLatch(1);
    private volatile String disconnectMessage;
    private volatile Exception disconnectFailure;

    /**
     * Frames waiting for this connection's sole writer.
     *
     * <p>The queue is the backpressure mechanism: there is no second semaphore
     * or caller-owned write lock. Transfer streaming deliberately bypasses it;
     * see {@link #write(long, InputStream, ConnectionGovernor,
     * ConnectionReporter, CancellationSignal)}.
     */
    private final ArrayBlockingQueue<FrameWrite> frameWrites;

    private final Object frameWriterLifecycle = new Object();
    private final AtomicBoolean frameWriterStarted = new AtomicBoolean();
    private volatile boolean frameWritesAccepted;
    private volatile FrameWrite activeFrameWrite;

    private final AtomicBoolean closeStarted = new AtomicBoolean();
    private volatile long lastActivityNanos = System.nanoTime();

    /**
     * The {@code SO_TIMEOUT} currently applied, so a read that does not need to
     * change it makes no syscall. Tracks what the constructor and
     * {@link #setStreamTimeouts()} install.
     */
    private int appliedReadTimeoutMillis = CANCELLATION_POLL_MILLIS;

    private volatile ConnectionState state = ConnectionState.PENDING;
    private volatile ConnectionType type = ConnectionType.UNCLASSIFIED;
    private volatile boolean writeQueueFull;

    /**
     * Whose sweep this connection is liveness- and inactivity-checked by.
     *
     * <p>One sweep task covers every connection a client has; see
     * {@link ConnectionMonitor}. Supplied rather than reached for: it used to be
     * a static field on this class, which made every client in the JVM share one
     * and gave none of them anything to shut down.
     */
    private final ConnectionMonitor monitor;

    private final ExecutorService ioExecutor;
    private final boolean ownsExecutor;

    protected InetSocketAddress ipEndpoint;
    protected final ConnectionOptions options;
    protected volatile NetworkStream stream;
    protected volatile TcpClient tcpClient;

    /** Executor shared by this connection's read and write loops. */
    protected final ExecutorService ioExecutor() {
        return ioExecutor;
    }

    /**
     * Creates a connection over an optional existing TCP client.
     *
     * @param ipEndpoint where the peer is
     * @param options the connection options, or {@code null} for the defaults
     * @param tcpClient an established client to adopt, or {@code null}
     * @param monitor the client's connection monitor; required, because a
     *     connection nobody sweeps never notices that it has gone idle or that
     *     its transport has gone away
     */
    public SocketConnection(
            InetSocketAddress ipEndpoint, ConnectionOptions options, TcpClient tcpClient, ConnectionMonitor monitor) {
        this(
                ipEndpoint,
                options,
                tcpClient,
                monitor,
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
                        .name("soulseek-standalone-connection-", 0)
                        .factory()),
                true);
    }

    /** Creates a connection sharing its client's I/O executor. */
    public SocketConnection(
            InetSocketAddress ipEndpoint,
            ConnectionOptions options,
            TcpClient tcpClient,
            ConnectionMonitor monitor,
            ExecutorService ioExecutor) {
        this(ipEndpoint, options, tcpClient, monitor, ioExecutor, false);
    }

    private SocketConnection(
            InetSocketAddress ipEndpoint,
            ConnectionOptions options,
            TcpClient tcpClient,
            ConnectionMonitor monitor,
            ExecutorService ioExecutor,
            boolean ownsExecutor) {
        this.monitor = Objects.requireNonNull(monitor, "monitor");
        this.ioExecutor = Objects.requireNonNull(ioExecutor, "ioExecutor");
        this.ownsExecutor = ownsExecutor;
        this.ipEndpoint = ipEndpoint;
        this.options = options == null ? new ConnectionOptions() : options;
        this.tcpClient = tcpClient == null ? new TcpClientAdapter() : tcpClient;
        // ArrayBlockingQueue requires a positive physical capacity. A
        // non-positive configured capacity retains the old observable
        // behaviour (every framed write times out/drops) through the explicit
        // check in enqueueFrameWrite.
        frameWrites = new ArrayBlockingQueue<>(Math.max(1, this.options.writeQueueSize()));

        try {
            this.options.configureSocket().configure(this.tcpClient.getClient());
            setSocketTimeout(CANCELLATION_POLL_MILLIS);

            if (this.tcpClient.isConnected()) {
                state = ConnectionState.CONNECTED;
                startTimers();
                stream = this.tcpClient.getStream();
                setStreamTimeouts();
                startFrameWriter();
            }
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Subscription subscribe(Kind kind, Consumer<? super T> listener) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(listener, "listener");
        return switch (kind) {
            case CONNECTED -> Subscriptions.add(connectedListeners, (Consumer<? super Connection>) listener);
            case DATA_READ -> Subscriptions.add(dataReadListeners, (Consumer<? super ConnectionDataEvent>) listener);
            case DATA_WRITTEN ->
                Subscriptions.add(dataWrittenListeners, (Consumer<? super ConnectionDataEvent>) listener);
            case DISCONNECTED ->
                Subscriptions.add(disconnectedListeners, (Consumer<? super ConnectionDisconnectedEvent>) listener);
            case STATE_CHANGED ->
                Subscriptions.add(stateChangedListeners, (Consumer<? super ConnectionStateChangedEvent>) listener);
        };
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
    public ConnectionType getType() {
        return type;
    }

    @Override
    public void setType(ConnectionType type) {
        this.type = type;
    }

    @Override
    public int getWriteQueueDepth() {
        int queued = 0;
        for (FrameWrite write : frameWrites) {
            if (write != FrameWrite.STOP) {
                queued++;
            }
        }
        FrameWrite active = activeFrameWrite;
        return queued + (active == null || !active.isWriting() ? 0 : 1);
    }

    /**
     * Connects to the configured endpoint on the calling thread.
     *
     * <p>The connect attempt itself still runs on a thread of its own, because
     * the caller has to be able to give up on it: a lapsed connect timeout, or
     * a cancellation, abandons the attempt rather than waiting for the
     * operating system to finish it. That is the one thing here a second thread
     * genuinely buys, and it is now the only one — the two that used to sit
     * around it, one to bridge the transport's future and one to hold the
     * caller's, are gone along with the shared timer this scheduled its
     * deadline on.
     */
    @Override
    public void connect(CancellationSignal cancellationSignal) throws InterruptedException, TimeoutException {
        if (state != ConnectionState.PENDING && state != ConnectionState.DISCONNECTED) {
            throw new IllegalStateException("Invalid attempt to connect a connected or "
                    + "transitioning connection (current state: "
                    + state + ")");
        }
        CancellationSignal token = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;

        changeState(ConnectionState.CONNECTING, "Connecting to " + formatEndpoint(ipEndpoint), null);

        try {
            Duration timeout = options.connectTimeout();
            if (timeout != null && timeout.isNegative()) {
                throw new IllegalArgumentException("connectTimeout must not be negative: " + timeout);
            }
            awaitTransportConnect(token, timeout);
            startTimers();
            stream = tcpClient.getStream();
            setStreamTimeouts();
            startFrameWriter();
            changeState(ConnectionState.CONNECTED, "Connected to " + formatEndpoint(ipEndpoint), null);
        } catch (Exception exception) {
            throw Failures.rethrow(handleConnectFailure(exception));
        }
    }

    /**
     * Runs the transport connect and waits for it, the deadline, or
     * cancellation — whichever lands first.
     *
     * <p>A one-slot queue rather than a future: the first outcome offered wins
     * and the rest are dropped, which is exactly what completing a future once
     * did, without the composition.
     */
    private void awaitTransportConnect(CancellationSignal token, Duration timeout) throws Exception {
        ArrayBlockingQueue<Object> gate = new ArrayBlockingQueue<>(1);
        Object connected = new Object();
        ioExecutor.execute(() -> {
            try {
                ProxyOptions proxy = options.proxyOptions();
                if (proxy != null) {
                    tcpClient.connectThroughProxy(
                            proxy.ipEndpoint().getAddress(),
                            proxy.ipEndpoint().getPort(),
                            ipEndpoint.getAddress(),
                            ipEndpoint.getPort(),
                            proxy.username(),
                            proxy.password(),
                            token);
                } else {
                    tcpClient.connect(ipEndpoint.getAddress(), ipEndpoint.getPort());
                }
                gate.offer(connected);
            } catch (Throwable failure) {
                gate.offer(failure);
            }
        });

        CancellationSubscription registration =
                token.register(() -> gate.offer(new CancellationException("Operation cancelled")));
        Object outcome;
        try {
            outcome = timeout == null ? gate.take() : gate.poll(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            throw new InterruptedOperationException("Operation cancelled", interrupted);
        } finally {
            registration.close();
        }

        if (outcome == null) {
            throw new TimeoutException("Operation timed out after " + timeout);
        }
        if (outcome instanceof Throwable failure) {
            throw asException(failure);
        }
    }

    /**
     * Disconnects, publishing the state and disconnected events.
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
        // Make the terminal state visible before releasing parked writers. The
        // disconnected event still follows transport teardown below, but an
        // exceptional send never returns while the state says transitioning.
        state = ConnectionState.DISCONNECTED;
        stopFrameWriter(() -> frameWriteTeardownFailure(reason, exception));
        closeTransport();

        publishStateChanged(ConnectionState.DISCONNECTING, ConnectionState.DISCONNECTED, reason, exception);
        publishDisconnected(reason, exception);
    }

    @Override
    public TcpClient handoffTcpClient() {
        stopFrameWriter(() -> new ConnectionWriteException("Write aborted because the transport was handed off"));
        TcpClient result = tcpClient;
        tcpClient = null;
        stream = null;
        return result;
    }

    @Override
    public byte[] read(long length, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        return read(length, null, cancellationSignal);
    }

    /**
     * Reads {@code length} bytes on the calling thread, reporting progress to
     * one listener scoped to this read in addition to the registered data-read
     * listeners.
     *
     * <p>Callers that want progress for a single read use this instead of
     * adding and removing themselves from the shared listener list around it.
     * That pattern copied the backing {@link CopyOnWriteArrayList} twice per
     * call, which on the framed read path meant twice per protocol message.
     *
     * @param length the number of bytes to read
     * @param scopedProgress the progress listener for this read, or {@code null}
     * @param cancellationSignal the cancellation signal
     * @return the bytes read
     */
    protected final byte[] read(
            long length, Consumer<ConnectionDataEvent> scopedProgress, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        validateRead(length);
        CancellationSignal token = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        readTo(length, output, SocketConnection::grantAll, null, scopedProgress, token);
        return output.toByteArray();
    }

    @Override
    public void read(
            long length,
            OutputStream outputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        if (length < 0) {
            throw new IllegalArgumentException("length must be greater than or equal to zero: " + length);
        }
        Objects.requireNonNull(outputStream, "outputStream");
        validateConnected();
        CancellationSignal token = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
        ConnectionGovernor effectiveGovernor = governor == null ? SocketConnection::grantAll : governor;
        readTo(length, outputStream, effectiveGovernor, reporter, null, token);
    }

    @Override
    public String awaitDisconnect(CancellationSignal cancellationSignal) throws InterruptedException, TimeoutException {
        if (cancellationSignal != null) {
            cancellationSignal.register(() -> disconnect(null, new CancellationException("Operation cancelled")));
        }
        try {
            disconnected.await();
        } catch (InterruptedException interrupted) {
            if (disconnected.getCount() != 0) {
                throw interrupted;
            }
            Thread.currentThread().interrupt();
        }
        if (disconnectFailure != null) {
            throw Failures.rethrow(disconnectFailure);
        }
        return disconnectMessage;
    }

    @Override
    public void write(byte[] bytes, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        beginWrite(bytes, cancellationSignal).await();
    }

    @Override
    public PendingWrite beginWrite(byte[] bytes, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("data must not be empty");
        }
        validateConnected();
        CancellationSignal token = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
        // The array is adopted, not copied: every caller hands over a freshly
        // built message and none reads or reuses it afterwards. The defensive
        // clone this replaces was one full copy of every frame ever sent.
        FrameWrite write = new FrameWrite(bytes);
        enqueueFrameWrite(write, token);
        return () -> awaitFrameWrite(write, token);
    }

    @Override
    public void write(
            long length,
            InputStream inputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        // Zero is rejected, not just negatives: a streaming write exists to
        // move transfer bytes, and a zero-length one is a caller bug.
        if (length <= 0) {
            throw new IllegalArgumentException("length must be greater than zero: " + length);
        }
        Objects.requireNonNull(inputStream, "inputStream");
        validateConnected();
        CancellationSignal token = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
        ConnectionGovernor effectiveGovernor = governor == null ? SocketConnection::grantAll : governor;
        writeStreaming(length, inputStream, effectiveGovernor, reporter, token);
    }

    @Override
    public void close() {
        // An atomic claim rather than a monitor, because disconnect() below
        // runs user listeners and must not do so under a library lock.
        if (!closeStarted.compareAndSet(false, true)) {
            return;
        }
        // Stackless: closing is a decision, not a fault, and this runs once
        // per connection at the churn rate of the whole peer mesh.
        disconnect(
                "SocketConnection is being closed",
                Failures.stacklessIllegalState(getClass().getSimpleName() + " has been closed"));
        stopTimers();
        closeTransport();
        if (ownsExecutor) {
            ioExecutor.shutdown();
        }
    }

    /**
     * Changes state and publishes the matching source events.
     *
     * <p>Callers must not hold a library lock: this invokes user listeners.
     */
    protected void changeState(ConnectionState newState, String message, Exception exception) {
        ConnectionState previousState = state;
        state = newState;

        publishStateChanged(previousState, newState, message, exception);
        if (newState == ConnectionState.CONNECTED) {
            for (Consumer<? super Connection> listener : connectedListeners) {
                listener.accept(this);
            }
        } else if (newState == ConnectionState.DISCONNECTED) {
            publishDisconnected(message, exception);
        }
    }

    /** Publishes the state-changed event. Must be called with no lock held. */
    private void publishStateChanged(
            ConnectionState previousState, ConnectionState newState, String message, Exception exception) {
        ConnectionStateChangedEvent eventData =
                new ConnectionStateChangedEvent(this, previousState, newState, message, exception);
        for (Consumer<? super ConnectionStateChangedEvent> listener : stateChangedListeners) {
            listener.accept(eventData);
        }
    }

    /**
     * Publishes the disconnected event and releases the disconnect latch. Must be
     * called with no lock held.
     */
    private void publishDisconnected(String message, Exception exception) {
        ConnectionDisconnectedEvent eventData = new ConnectionDisconnectedEvent(this, message, exception);
        for (Consumer<? super ConnectionDisconnectedEvent> listener : disconnectedListeners) {
            listener.accept(eventData);
        }
        // Written before the latch drops and only for the first caller through,
        // so a waiter that is released sees the reason that released it and a
        // second disconnect cannot rewrite it.
        if (disconnected.getCount() > 0) {
            synchronized (this) {
                if (disconnected.getCount() > 0) {
                    disconnectMessage = message;
                    disconnectFailure = exception;
                    disconnected.countDown();
                }
            }
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

    /** Returns whether this connection has been closed. */
    protected final boolean isClosed() {
        return closeStarted.get();
    }

    /** Sets state for derived source ports that adopt a connection. */
    protected final void setState(ConnectionState value) {
        state = value;
    }

    private void readTo(
            long length,
            OutputStream outputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            Consumer<ConnectionDataEvent> scopedProgress,
            CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        resetInactivityTime();
        // Sized to the request, not to the configured maximum. The framed read
        // loop asks for 4 bytes, then the code, then the payload; a full
        // read-buffer allocation for each of those was 48 KiB of garbage per
        // 40-byte protocol message.
        byte[] buffer = new byte[(int) Math.min(options.readBufferSize(), Math.max(1L, length))];
        long totalBytesRead = 0;

        try {
            applyReadTimeout(cancellationSignal);
            while (!closeStarted.get() && totalBytesRead < length) {
                cancellationSignal.throwIfCancellationRequested();
                long bytesRemaining = length - totalBytesRead;
                int bytesToRead = bytesRemaining >= buffer.length ? buffer.length : (int) bytesRemaining;
                int bytesGranted = Math.min(bytesToRead, governor.grant(bytesToRead, cancellationSignal));

                int bytesRead;
                try {
                    // Called directly on this thread. It used to be dispatched
                    // onto a second virtual thread that this one then blocked
                    // on, which bought nothing: both threads were doing the
                    // same read.
                    bytesRead = stream.read(buffer, 0, bytesGranted);
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
        } catch (Exception actual) {
            disconnect("Read error: " + actual.getMessage(), actual);
            if (actual instanceof TimeoutException timeout) {
                throw timeout;
            }
            if (actual instanceof CancellationException cancelled) {
                throw cancelled;
            }
            throw new ConnectionReadException(
                    "Failed to read " + length + " bytes from "
                            + formatEndpoint(ipEndpoint) + ": "
                            + actual.getMessage(),
                    actual);
        }
    }

    /**
     * Streams transfer data on its dedicated connection.
     *
     * <p>This path is intentionally caller-thread/signal driven internally: its
     * public owner already dispatches the transfer on a library worker, and
     * cancellation tears down this single-purpose connection. Framed shared-
     * connection traffic must use {@link #enqueueFrameWrite(FrameWrite,
     * CancellationSignal)} instead.
     */
    private void writeStreaming(
            long length,
            InputStream inputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        try {
            resetInactivityTime();
            byte[] buffer = new byte[(int) Math.min(options.writeBufferSize(), Math.max(1L, length))];
            long totalBytesWritten = 0;

            while (totalBytesWritten < length) {
                cancellationSignal.throwIfCancellationRequested();
                boolean closing = closeStarted.get();
                if (closing || state == ConnectionState.DISCONNECTING || state == ConnectionState.DISCONNECTED) {
                    throw new ConnectionWriteException("Write aborted after " + totalBytesWritten
                            + " bytes written; the connection has "
                            + "been or is being "
                            + (closing ? "closed" : "disconnected"));
                }
                long bytesRemaining = length - totalBytesWritten;
                int bytesToRead = bytesRemaining >= buffer.length ? buffer.length : (int) bytesRemaining;
                int bytesGranted = Math.min(bytesToRead, governor.grant(bytesToRead, cancellationSignal));
                int bytesRead = inputStream.read(buffer, 0, bytesGranted);
                if (bytesRead < 0) {
                    bytesRead = 0;
                }
                stream.write(buffer, 0, bytesRead);
                totalBytesWritten += bytesRead;
                if (reporter != null) {
                    reporter.report(bytesToRead, bytesGranted, bytesRead);
                }
                emitProgress(dataWrittenListeners, null, totalBytesWritten, length, cancellationSignal);
                resetInactivityTime();
            }
        } catch (Exception actual) {
            disconnect("Write error: " + actual.getMessage(), actual);
            if (actual instanceof TimeoutException timeout) {
                throw timeout;
            }
            if (actual instanceof CancellationException cancelled) {
                throw cancelled;
            }
            throw new ConnectionWriteException(
                    "Failed to write " + length + " bytes to "
                            + formatEndpoint(ipEndpoint) + ": "
                            + actual.getMessage(),
                    actual);
        }
    }

    /** Starts the one persistent writer owned by this connection lifecycle. */
    private void startFrameWriter() {
        synchronized (frameWriterLifecycle) {
            if (frameWriterStarted.get()) {
                return;
            }
            frameWritesAccepted = true;
            frameWriterStarted.set(true);
            ioExecutor.execute(this::runFrameWriter);
        }
    }

    /** Drains framed writes until connection teardown posts the stop marker. */
    private void runFrameWriter() {
        while (true) {
            FrameWrite write;
            try {
                // This is a library-owned socket thread. It is never
                // interrupted; teardown wakes it with STOP and closes the
                // transport if a write is blocked in the socket.
                write = frameWrites.take();
            } catch (InterruptedException impossible) {
                throw new IllegalStateException("The connection-owned writer was interrupted", impossible);
            }
            if (write == FrameWrite.STOP) {
                return;
            }
            activeFrameWrite = write;
            if (!write.start()) {
                activeFrameWrite = null;
                continue;
            }

            try {
                writeFrame(write.bytes());
                write.succeed();
            } catch (Exception exception) {
                Exception failure = mapFrameWriteFailure(write.bytes().length, exception);
                disconnect("Write error: " + failure.getMessage(), failure);
                write.fail(failure);
            } catch (Throwable failure) {
                RuntimeException actual = new RuntimeException("Unexpected framed-write failure", failure);
                disconnect("Write error: " + actual.getMessage(), actual);
                write.fail(actual);
            } finally {
                activeFrameWrite = null;
            }
        }
    }

    /** Writes one complete frame without consulting its caller's cancellation. */
    private void writeFrame(byte[] bytes) throws Exception {
        resetInactivityTime();
        boolean closing = closeStarted.get();
        if (closing || state == ConnectionState.DISCONNECTING || state == ConnectionState.DISCONNECTED) {
            throw new ConnectionWriteException("Write aborted before the frame reached the socket; the connection "
                    + "has been or is being " + (closing ? "closed" : "disconnected"));
        }
        stream.write(bytes, 0, bytes.length);
        emitProgress(dataWrittenListeners, null, bytes.length, bytes.length, CancellationSignal.none());
        resetInactivityTime();
    }

    private Exception mapFrameWriteFailure(int length, Exception actual) {
        if (actual instanceof TimeoutException || actual instanceof CancellationException) {
            return actual;
        }
        if (actual instanceof ConnectionWriteException) {
            return actual;
        }
        return new ConnectionWriteException(
                "Failed to write " + length + " bytes to " + formatEndpoint(ipEndpoint) + ": " + actual.getMessage(),
                actual);
    }

    private Exception frameWriteTeardownFailure(String reason, Exception exception) {
        if (exception instanceof ConnectionWriteException || exception instanceof CancellationException) {
            return exception;
        }
        String message = "Write aborted because the connection is disconnecting"
                + (reason == null || reason.isBlank() ? "" : ": " + reason);
        return exception == null
                ? new ConnectionWriteException(message)
                : new ConnectionWriteException(message, exception);
    }

    /** Enqueues a frame with the configured bounded backpressure wait. */
    private void enqueueFrameWrite(FrameWrite write, CancellationSignal cancellationSignal) {
        if (writeQueueFull || !frameWritesAccepted || options.writeQueueSize() <= 0) {
            dropFrameWrite(write);
            return;
        }

        boolean offered;
        try {
            offered =
                    CancellationInterrupts.interruptOnCancel(cancellationSignal, () -> offerFrame(write), accepted -> {
                        if (accepted) {
                            write.cancel(frameWrites);
                        }
                    });
        } catch (InterruptedException interrupted) {
            throw new InterruptedOperationException("Operation cancelled", interrupted);
        }
        if (!offered) {
            dropFrameWrite(write);
            return;
        }

        // Serialize the accepting-state check with teardown's drain. Either
        // teardown sees this request, or this request observes teardown and
        // removes itself; it cannot be stranded behind the stop marker.
        synchronized (frameWriterLifecycle) {
            if (!frameWritesAccepted) {
                write.cancel(frameWrites);
                throw new ConnectionWriteException("Write aborted because the connection is disconnecting");
            }
        }
    }

    /** Offers a frame within the configured backpressure window. */
    private boolean offerFrame(FrameWrite write) throws InterruptedException {
        Duration timeout = options.writeQueueTimeout();
        return timeout == null || timeout.isZero() || timeout.isNegative()
                ? frameWrites.offer(write)
                : frameWrites.offer(write, timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void dropFrameWrite(FrameWrite write) {
        writeQueueFull = true;
        ConnectionWriteDroppedException dropped =
                new ConnectionWriteDroppedException("Dropped buffered message to " + formatEndpoint(ipEndpoint)
                        + "; the write buffer stayed full for "
                        + options.writeQueueTimeout());
        write.fail(dropped);
        disconnect("The write buffer is full", dropped);
        throw dropped;
    }

    /** Waits only on the request completion; the writer owns all socket I/O. */
    private void awaitFrameWrite(FrameWrite write, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        // The signal that can never fire needs no listener; skipping it spares
        // the capturing lambda on the common non-cancellable frame.
        CancellationSubscription registration = cancellationSignal == CancellationSignal.none()
                ? null
                : cancellationSignal.register(() -> write.cancel(frameWrites));
        try {
            try {
                write.await();
            } catch (InterruptedException interrupted) {
                if (write.cancel(frameWrites)) {
                    throw new InterruptedOperationException("Operation cancelled", interrupted);
                }
                // Completion committed first, so this interrupt belongs to the
                // caller's enclosing work rather than this write.
                Thread.currentThread().interrupt();
            }
        } finally {
            if (registration != null) {
                registration.close();
            }
        }
        write.raiseOutcome();
    }

    /**
     * Stops acceptance and settles every active or queued frame exactly once.
     *
     * <p>The failure is supplied, not passed: most teardowns find nothing
     * active and nothing queued, and constructing an exception for every one
     * of them was the second-largest exception source in the JFR baseline.
     * The supplier runs at most once, so every settled frame still shares the
     * one instance they always did.
     */
    private void stopFrameWriter(java.util.function.Supplier<Exception> failure) {
        if (!frameWriterStarted.get()) {
            return;
        }

        synchronized (frameWriterLifecycle) {
            if (!frameWritesAccepted) {
                return;
            }
            frameWritesAccepted = false;
            Exception settled = null;
            FrameWrite active = activeFrameWrite;
            if (active != null) {
                settled = failure.get();
                active.fail(settled);
            }
            for (FrameWrite pending; (pending = frameWrites.poll()) != null; ) {
                if (pending != FrameWrite.STOP) {
                    if (settled == null) {
                        settled = failure.get();
                    }
                    pending.fail(settled);
                }
            }
            // The drain above guarantees room. Producers that passed their
            // pre-offer state check either remove themselves after observing
            // frameWritesAccepted=false or are drained under this same lock.
            frameWrites.offer(FrameWrite.STOP);
        }
    }

    private void validateRead(long length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must not be negative: " + length);
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

    /**
     * Applies the {@code SO_TIMEOUT} the pending read needs.
     *
     * <p>A read governed by {@link CancellationSignal#none()} can never be
     * cancelled, so there is nothing for it to wake up and poll for. The framed
     * message loop passes exactly that for all three of its reads per frame, so
     * on an idle peer or distributed connection the 250 ms poll did nothing but
     * throw: four {@code SocketTimeoutException}s a second, each filling in a
     * stack trace, for the life of the connection. A recorded client held
     * around twenty such connections and threw 5.9 million of them in
     * eighteen hours.
     *
     * <p>Those reads block indefinitely instead. Liveness there was never the
     * timeout's job — the periodic monitor owns it, and it disconnects, which
     * closes the transport out from under the blocked read.
     *
     * <p>A cancellable read keeps the poll. Its expiry leaves the socket usable
     * and loses no bytes, which is what lets a transfer be cancelled without
     * tearing down the connection carrying it.
     */
    private void applyReadTimeout(CancellationSignal cancellationSignal) throws IOException {
        // -1, not 0: NetworkStream spells "no timeout" as -1 and rejects 0.
        int desired = cancellationSignal == CancellationSignal.none() ? NO_READ_TIMEOUT : CANCELLATION_POLL_MILLIS;
        if (desired == appliedReadTimeoutMillis || stream == null) {
            return;
        }
        stream.setReadTimeout(desired);
        appliedReadTimeoutMillis = desired;
    }

    private void setStreamTimeouts() throws IOException {
        // The read timeout is the cancellation poll interval, not the
        // inactivity budget; the periodic monitor owns inactivity now.
        // applyReadTimeout narrows it per read.
        stream.setReadTimeout(CANCELLATION_POLL_MILLIS);
        appliedReadTimeoutMillis = CANCELLATION_POLL_MILLIS;
        // SO_TIMEOUT does not apply to writes in Java, so this stays
        // informational; write cancellation is checked between chunks.
        stream.setWriteTimeout(durationMillisOrInfinite(options.inactivityTimeout()));
    }

    private void startTimers() {
        monitor.register(this);
    }

    private void stopTimers() {
        monitor.unregister(this);
    }

    /**
     * Returns the monitor cadence.
     *
     * <p>Capped at the original 250 ms watchdog interval so liveness detection
     * is unchanged, and scaled down for short inactivity timeouts so those stay
     * about as precise as the old dedicated one-shot timer.
     */
    int monitorIntervalMillis() {
        Duration timeout = options.inactivityTimeout();
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            return MAX_MONITOR_INTERVAL_MILLIS;
        }
        return Math.clamp(
                Math.toIntExact(timeout.toMillis() / 4), MIN_MONITOR_INTERVAL_MILLIS, MAX_MONITOR_INTERVAL_MILLIS);
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

        Duration timeout = options.inactivityTimeout();
        if (timeout != null && timeout.isPositive()) {
            Duration idle = Duration.ofNanos(System.nanoTime() - lastActivityNanos);
            if (idle.compareTo(timeout) >= 0) {
                TimeoutException exception = new TimeoutException("Inactivity timeout of " + timeout + " was reached");
                disconnect(exception.getMessage(), exception);
            }
        }
    }

    private static int durationMillisOrInfinite(Duration duration) {
        return duration == null ? -1 : Math.toIntExact(duration.toMillis());
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

    private Exception handleConnectFailure(Exception actual) {
        disconnect("SocketConnection Error: " + actual.getMessage(), actual);
        if (actual instanceof TimeoutException || actual instanceof CancellationException) {
            return actual;
        }
        return new ConnectionException(
                "Failed to connect to " + formatEndpoint(ipEndpoint) + ": " + actual.getMessage(), actual);
    }

    private static int grantAll(int requestedBytes, CancellationSignal cancellationSignal) {
        return Integer.MAX_VALUE;
    }

    /**
     * Publishes a progress event to the registered listeners and, optionally, to
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
            CopyOnWriteArrayList<Consumer<? super ConnectionDataEvent>> listeners,
            Consumer<? super ConnectionDataEvent> scopedListener,
            long currentLength,
            long totalLength,
            CancellationSignal cancellationSignal) {
        if (listeners.isEmpty() && scopedListener == null) {
            return;
        }
        ConnectionDataEvent eventData = new ConnectionDataEvent(this, currentLength, totalLength);
        Runnable dispatch = () -> {
            for (Consumer<? super ConnectionDataEvent> listener : listeners) {
                listener.accept(eventData);
            }
            if (scopedListener != null) {
                scopedListener.accept(eventData);
            }
        };
        // Inline: these listeners are the library's own progress counters, and
        // the one place consumer code used to be reachable from a read loop is
        // now behind the event bus's own delivery thread.
        dispatch.run();
    }

    /** One immutable frame plus the two atomic lifecycles it participates in. */
    private static final class FrameWrite {
        private static final FrameWrite STOP = new FrameWrite(new byte[0]);

        private final byte[] bytes;
        private final CountDownLatch settled = new CountDownLatch(1);
        private final AtomicReference<FrameState> frameState = new AtomicReference<>(FrameState.QUEUED);
        private final AtomicReference<WaitState> waitState = new AtomicReference<>(WaitState.WAITING);
        private volatile Exception failure;

        private FrameWrite(byte[] bytes) {
            this.bytes = bytes;
        }

        private byte[] bytes() {
            return bytes;
        }

        private boolean start() {
            return frameState.compareAndSet(FrameState.QUEUED, FrameState.WRITING);
        }

        private boolean isWriting() {
            return frameState.get() == FrameState.WRITING;
        }

        private void succeed() {
            frameState.compareAndSet(FrameState.WRITING, FrameState.SUCCEEDED);
            settle(WaitState.SUCCEEDED, null);
        }

        private void fail(Exception exception) {
            frameState.getAndUpdate(current -> switch (current) {
                case QUEUED, WRITING -> FrameState.FAILED;
                case CANCELLED, SUCCEEDED, FAILED -> current;
            });
            settle(WaitState.FAILED, exception);
        }

        /**
         * Lets cancellation atomically win the caller's wait. A queued frame is
         * also removed; a frame already being written is deliberately left for
         * the connection-owned writer to finish.
         */
        private boolean cancel(ArrayBlockingQueue<FrameWrite> queue) {
            if (!waitState.compareAndSet(WaitState.WAITING, WaitState.CANCELLED)) {
                return false;
            }
            if (frameState.compareAndSet(FrameState.QUEUED, FrameState.CANCELLED)) {
                queue.remove(this);
            }
            settled.countDown();
            return true;
        }

        private void await() throws InterruptedException {
            settled.await();
        }

        private void raiseOutcome() throws InterruptedException, TimeoutException {
            switch (waitState.get()) {
                case SUCCEEDED -> {
                    return;
                }
                case FAILED -> throw Failures.rethrow(failure);
                case CANCELLED -> throw new CancellationException("Operation cancelled");
                case WAITING -> throw new IllegalStateException("Framed write wait returned before settling");
            }
        }

        private void settle(WaitState outcome, Exception exception) {
            if (waitState.compareAndSet(WaitState.WAITING, outcome)) {
                failure = exception;
                settled.countDown();
            }
        }
    }

    private enum FrameState {
        QUEUED,
        WRITING,
        SUCCEEDED,
        FAILED,
        CANCELLED
    }

    private enum WaitState {
        WAITING,
        SUCCEEDED,
        FAILED,
        CANCELLED
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
}
