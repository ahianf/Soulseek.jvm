// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.Subscription;
import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.common.Text;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.events.Subscriptions;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.network.tcp.ConnectionDataEvent;
import dev.slsk.internal.network.tcp.ConnectionKey;
import dev.slsk.internal.network.tcp.ConnectionMonitor;
import dev.slsk.internal.network.tcp.SocketConnection;
import dev.slsk.internal.network.tcp.SocketConnector;
import dev.slsk.internal.options.ConnectionOptions;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/** Provides framed client connections to the Soulseek network. */
public final class DefaultMessageConnection extends SocketConnection implements MessageConnection {

    // Nicotine+ applies these limits before buffering an incoming frame.
    // Browse and user-information responses are the only peer messages that
    // legitimately need the larger allowance.
    private static final int MAX_INCOMING_MESSAGE_SIZE_LARGE = 448 * 1024 * 1024;
    private static final int MAX_INCOMING_MESSAGE_SIZE_MEDIUM = 16 * 1024 * 1024;
    private static final int MAX_INCOMING_MESSAGE_SIZE_SMALL = 16 * 1024;
    private static final int BROWSE_RESPONSE_CODE = 5;
    private static final int INFO_RESPONSE_CODE = 16;

    private final CopyOnWriteArrayList<Consumer<? super MessageDataEvent>> messageDataReadListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super MessageEvent>> messageReadListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super MessageReceivedEvent>> messageReceivedListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<? super MessageEvent>> messageWrittenListeners =
            new CopyOnWriteArrayList<>();

    private final int codeLength;
    private final String username;
    private volatile boolean readingContinuously;

    /** Creates a server connection. */
    public DefaultMessageConnection(
            InetSocketAddress ipEndpoint,
            ConnectionOptions options,
            int codeLength,
            SocketConnector connector,
            ConnectionMonitor monitor) {
        super(ipEndpoint, options, connector, monitor);
        this.codeLength = codeLength;
        username = "";
        bindConnectedReadLoop();
    }

    /** Creates a server connection sharing its client's I/O executor. */
    public DefaultMessageConnection(
            InetSocketAddress ipEndpoint,
            ConnectionOptions options,
            int codeLength,
            SocketConnector connector,
            ConnectionMonitor monitor,
            ExecutorService ioExecutor) {
        super(ipEndpoint, options, connector, monitor, ioExecutor);
        this.codeLength = codeLength;
        username = "";
        bindConnectedReadLoop();
    }

    /** Creates a peer connection. */
    public DefaultMessageConnection(
            String username,
            InetSocketAddress ipEndpoint,
            ConnectionOptions options,
            int codeLength,
            SocketConnector connector,
            ConnectionMonitor monitor) {
        super(ipEndpoint, options, connector, monitor);
        this.codeLength = codeLength;
        Text.requireText(username, "username");
        this.username = username;
        bindConnectedReadLoop();
    }

    /** Creates a peer connection sharing its client's I/O executor. */
    public DefaultMessageConnection(
            String username,
            InetSocketAddress ipEndpoint,
            ConnectionOptions options,
            int codeLength,
            SocketConnector connector,
            ConnectionMonitor monitor,
            ExecutorService ioExecutor) {
        super(ipEndpoint, options, connector, monitor, ioExecutor);
        this.codeLength = codeLength;
        Text.requireText(username, "username");
        this.username = username;
        bindConnectedReadLoop();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Subscription subscribe(MessageKind kind, Consumer<? super T> listener) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(listener, "listener");
        return switch (kind) {
            case DATA_READ ->
                Subscriptions.add(messageDataReadListeners, (Consumer<? super MessageDataEvent>) listener);
            case READ -> Subscriptions.add(messageReadListeners, (Consumer<? super MessageEvent>) listener);
            case RECEIVED ->
                Subscriptions.add(messageReceivedListeners, (Consumer<? super MessageReceivedEvent>) listener);
            case WRITTEN -> Subscriptions.add(messageWrittenListeners, (Consumer<? super MessageEvent>) listener);
        };
    }

    @Override
    public int getCodeLength() {
        return codeLength;
    }

    @Override
    public boolean isServerConnection() {
        return username.isEmpty();
    }

    @Override
    public ConnectionKey getKey() {
        return new ConnectionKey(username, getIpEndpoint());
    }

    @Override
    public boolean isReadingContinuously() {
        return readingContinuously;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public void startReadingContinuously() {
        if (!readingContinuously) {
            startReadLoop();
        }
    }

    /**
     * Starts the read loop on a virtual thread of its own and routes its
     * failure into the connection's own failure channel.
     *
     * <p>This loop used to be started with a helper that attached
     * {@code exceptionally(e -> null)} and dropped the throwable on the floor —
     * on the single most important loop in the library. Most failures already
     * disconnect on the way out of {@code readTo}, but anything else, a
     * throwing listener included, vanished without trace.
     *
     * <p>Disconnecting is the right channel rather than a log line: it is what
     * {@code ConnectionDisconnectedEvent} and {@code awaitDisconnect} already
     * report, so the failure reaches whoever owns the connection. When the
     * connection has already gone down, this is a no-op and the original reason
     * is preserved.
     *
     * <p>The thread is the loop's own, not a future's. The loop runs until the
     * connection dies, so there is nothing for a caller to compose onto and the
     * future it used to return existed only to carry the failure back here.
     */
    private void startReadLoop() {
        ioExecutor().execute(() -> {
            try {
                readContinuously();
            } catch (Throwable failure) {
                if (isClosed()) {
                    return;
                }
                Exception reported = failure instanceof Exception exception
                        ? exception
                        : new MessageException(failure.toString(), failure);
                disconnect("Read loop failed: " + reported.getMessage(), reported);
            }
        });
    }

    @Override
    public void write(OutgoingMessage message, CancellationSignal cancellationSignal)
            throws InterruptedException, java.util.concurrent.TimeoutException {
        Objects.requireNonNull(message, "message");
        byte[] bytes;
        try {
            bytes = message.toByteArray();
        } catch (Exception exception) {
            throw new MessageException("Failed to convert the message to a byte array", exception);
        }
        CancellationSignal token = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
        super.write(bytes, token);
        publishMessageWritten(bytes);
    }

    void readContinuously() throws InterruptedException, java.util.concurrent.TimeoutException {
        synchronized (this) {
            if (readingContinuously) {
                return;
            }
            readingContinuously = true;
        }
        // Holds the code of the message currently being read, so the scoped
        // progress listener can label its events. Confined to this loop's
        // single thread.
        byte[][] codeHolder = new byte[1][];
        Consumer<ConnectionDataEvent> payloadProgress =
                event -> publishMessageDataRead(codeHolder[0], event.currentLength(), event.totalLength());
        try {
            while (!isClosed()) {
                // Length and code are one fixed-size header. Reading them
                // together removes one socket read per frame without adding a
                // persistent connection buffer.
                byte[] header = new byte[4 + codeLength];
                readExactly(header, 0, header.length, null, CancellationSignal.none());
                int length = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN).getInt();
                if (length < codeLength) {
                    throw new MessageException(
                            "Invalid frame length " + length + " for a " + codeLength + "-byte message code");
                }
                byte[] codeBytes = Arrays.copyOfRange(header, 4, header.length);
                int maximumLength = maximumIncomingMessageSize(codeBytes);
                if (length > maximumLength) {
                    throw new MessageException(
                            "Incoming frame length " + length + " exceeds maximum " + maximumLength);
                }
                codeHolder[0] = codeBytes;

                publishMessageDataRead(codeBytes, 0, length - codeLength);
                publishMessageReceived(length, codeBytes);

                // Passed to the read rather than added to the shared
                // listener list and removed afterwards, which cost two
                // CopyOnWriteArrayList copies per message.
                byte[] message;
                try {
                    message = new byte[Math.addExact(4, length)];
                } catch (ArithmeticException | NegativeArraySizeException invalidLength) {
                    throw new MessageException("Invalid frame length: " + length, invalidLength);
                }
                System.arraycopy(header, 0, message, 0, header.length);
                readExactly(
                        message,
                        header.length,
                        length - codeLength,
                        payloadProgress,
                        CancellationSignal.none());
                publishMessageRead(message);
            }
        } finally {
            readingContinuously = false;
        }
    }

    private int maximumIncomingMessageSize(byte[] codeBytes) {
        if (isServerConnection()) {
            return MAX_INCOMING_MESSAGE_SIZE_LARGE;
        }
        if (codeLength == 1) {
            return MAX_INCOMING_MESSAGE_SIZE_SMALL;
        }
        int code = ByteBuffer.wrap(codeBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
        return code == BROWSE_RESPONSE_CODE || code == INFO_RESPONSE_CODE
                ? MAX_INCOMING_MESSAGE_SIZE_LARGE
                : MAX_INCOMING_MESSAGE_SIZE_MEDIUM;
    }

    private void bindConnectedReadLoop() {
        subscribe(Kind.CONNECTED, connection -> startReadLoop());
    }

    private void publishMessageDataRead(byte[] code, long currentLength, long totalLength) {
        MessageDataEvent eventData = new MessageDataEvent(this, code, currentLength, totalLength);
        dispatch(() -> {
            for (Consumer<? super MessageDataEvent> listener : messageDataReadListeners) {
                listener.accept(eventData);
            }
        });
    }

    private void publishMessageReceived(long length, byte[] code) {
        MessageReceivedEvent eventData = new MessageReceivedEvent(this, length, code);
        for (Consumer<? super MessageReceivedEvent> listener : messageReceivedListeners) {
            listener.accept(eventData);
        }
    }

    private void publishMessageRead(byte[] message) {
        MessageEvent eventData = new MessageEvent(this, message);
        dispatch(() -> {
            for (Consumer<? super MessageEvent> listener : messageReadListeners) {
                listener.accept(eventData);
            }
        });
    }

    private void publishMessageWritten(byte[] message) {
        MessageEvent eventData = new MessageEvent(this, message);
        dispatch(() -> {
            for (Consumer<? super MessageEvent> listener : messageWrittenListeners) {
                listener.accept(eventData);
            }
        });
    }

    /**
     * Runs a connection event's listeners inline.
     *
     * <p>They are the library's own — the message handlers — and they are
     * cheap. A per-connection switch used to send them to a virtual thread
     * instead, because consumer listeners were once reachable from here and a
     * slow one stalled the read loop. That is the event bus's job now, and it
     * does it for every facet rather than one connection at a time, so the
     * switch had nothing left to decide.
     */
    private void dispatch(Runnable event) {
        event.run();
    }
}
