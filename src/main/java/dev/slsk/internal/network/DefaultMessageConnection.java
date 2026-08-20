// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.Subscription;
import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.common.CommonUtils;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.events.Subscriptions;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.network.tcp.ConnectionDataEvent;
import dev.slsk.internal.network.tcp.ConnectionKey;
import dev.slsk.internal.network.tcp.ConnectionMonitor;
import dev.slsk.internal.network.tcp.SocketConnection;
import dev.slsk.internal.network.tcp.TcpClient;
import dev.slsk.internal.options.ConnectionOptions;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/** Provides framed client connections to the Soulseek network. */
public final class DefaultMessageConnection extends SocketConnection implements MessageConnection {

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
            TcpClient tcpClient,
            ConnectionMonitor monitor) {
        super(ipEndpoint, options, tcpClient, monitor);
        this.codeLength = codeLength;
        username = "";
        bindConnectedReadLoop();
    }

    /** Creates a server connection sharing its client's I/O executor. */
    public DefaultMessageConnection(
            InetSocketAddress ipEndpoint,
            ConnectionOptions options,
            int codeLength,
            TcpClient tcpClient,
            ConnectionMonitor monitor,
            ExecutorService ioExecutor) {
        super(ipEndpoint, options, tcpClient, monitor, ioExecutor);
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
            TcpClient tcpClient,
            ConnectionMonitor monitor) {
        super(ipEndpoint, options, tcpClient, monitor);
        this.codeLength = codeLength;
        CommonUtils.requireText(username, "username");
        this.username = username;
        bindConnectedReadLoop();
    }

    /** Creates a peer connection sharing its client's I/O executor. */
    public DefaultMessageConnection(
            String username,
            InetSocketAddress ipEndpoint,
            ConnectionOptions options,
            int codeLength,
            TcpClient tcpClient,
            ConnectionMonitor monitor,
            ExecutorService ioExecutor) {
        super(ipEndpoint, options, tcpClient, monitor, ioExecutor);
        this.codeLength = codeLength;
        CommonUtils.requireText(username, "username");
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
     * disconnect on the way out of {@code readInternal}, but anything else, a
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
                ByteArrayOutputStream message = new ByteArrayOutputStream();
                // Read on this thread. Each of these used to dispatch onto
                // a fresh virtual thread and block on the future, three
                // times per frame, on the path that carries distributed
                // search traffic.
                byte[] lengthBytes = read(4, null, CancellationSignal.none());
                int length = ByteBuffer.wrap(lengthBytes)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .getInt();
                message.writeBytes(lengthBytes);

                byte[] codeBytes = read(codeLength, null, CancellationSignal.none());
                codeHolder[0] = codeBytes;
                message.writeBytes(codeBytes);

                publishMessageDataRead(codeBytes, 0, length - codeLength);
                publishMessageReceived(length, codeBytes);

                // Passed to the read rather than added to the shared
                // listener list and removed afterwards, which cost two
                // CopyOnWriteArrayList copies per message.
                byte[] payload = read(length - codeLength, payloadProgress, CancellationSignal.none());
                message.writeBytes(payload);
                publishMessageRead(message.toByteArray());
            }
        } finally {
            readingContinuously = false;
        }
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
