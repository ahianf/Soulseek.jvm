// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.common.CommonUtils;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.network.tcp.ConnectionDataEvent;
import dev.slsk.internal.network.tcp.ConnectionEventListener;
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

/** Provides framed client connections to the Soulseek network. */
public final class DefaultMessageConnection extends SocketConnection implements MessageConnection {

    private final CopyOnWriteArrayList<MessageConnectionEventListener<MessageDataEvent>> messageDataReadListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<MessageConnectionEventListener<MessageEvent>> messageReadListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<MessageConnectionEventListener<MessageReceivedEvent>> messageReceivedListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<MessageConnectionEventListener<MessageEvent>> messageWrittenListeners =
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
        if (isNullOrWhiteSpace(username)) {
            throw new IllegalArgumentException(
                    "The username must not be a null or empty string, " + "or one consisting only of whitespace");
        }
        this.username = username;
        bindConnectedReadLoop();
    }

    @Override
    public void addMessageDataReadListener(MessageConnectionEventListener<MessageDataEvent> listener) {
        messageDataReadListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeMessageDataReadListener(MessageConnectionEventListener<MessageDataEvent> listener) {
        messageDataReadListeners.remove(listener);
    }

    @Override
    public void addMessageReadListener(MessageConnectionEventListener<MessageEvent> listener) {
        messageReadListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeMessageReadListener(MessageConnectionEventListener<MessageEvent> listener) {
        messageReadListeners.remove(listener);
    }

    @Override
    public void addMessageReceivedListener(MessageConnectionEventListener<MessageReceivedEvent> listener) {
        messageReceivedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeMessageReceivedListener(MessageConnectionEventListener<MessageReceivedEvent> listener) {
        messageReceivedListeners.remove(listener);
    }

    @Override
    public void addMessageWrittenListener(MessageConnectionEventListener<MessageEvent> listener) {
        messageWrittenListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeMessageWrittenListener(MessageConnectionEventListener<MessageEvent> listener) {
        messageWrittenListeners.remove(listener);
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
        NetworkExecutor.executor().execute(() -> {
            try {
                readContinuously();
            } catch (Throwable failure) {
                if (isDisposed()) {
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
        if (message == null) {
            throw new IllegalArgumentException("The specified message is null");
        }
        byte[] bytes;
        try {
            bytes = message.toByteArray();
        } catch (Exception exception) {
            throw new MessageException("Failed to convert the message to a byte array", exception);
        }
        CancellationSignal token = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
        super.write(bytes, token);
        raiseMessageWritten(bytes, token);
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
        ConnectionEventListener<ConnectionDataEvent> payloadProgress =
                (sender, args) -> raiseMessageDataRead(codeHolder[0], args.getCurrentLength(), args.getTotalLength());
        try {
            while (!isDisposed()) {
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

                raiseMessageDataRead(codeBytes, 0, length - codeLength);
                raiseMessageReceived(length, codeBytes);

                // Passed to the read rather than added to the shared
                // listener list and removed afterwards, which cost two
                // CopyOnWriteArrayList copies per message.
                byte[] payload = read(length - codeLength, payloadProgress, CancellationSignal.none());
                message.writeBytes(payload);
                raiseMessageRead(message.toByteArray());
            }
        } finally {
            readingContinuously = false;
        }
    }

    private void bindConnectedReadLoop() {
        addConnectedListener((sender, args) -> startReadLoop());
    }

    private void raiseMessageDataRead(byte[] code, long currentLength, long totalLength) {
        MessageDataEvent eventData = new MessageDataEvent(code, currentLength, totalLength);
        dispatch(
                () -> {
                    for (MessageConnectionEventListener<MessageDataEvent> listener : messageDataReadListeners) {
                        listener.handle(this, eventData);
                    }
                },
                CancellationSignal.none());
    }

    private void raiseMessageReceived(long length, byte[] code) {
        MessageReceivedEvent eventData = new MessageReceivedEvent(length, code);
        for (MessageConnectionEventListener<MessageReceivedEvent> listener : messageReceivedListeners) {
            listener.handle(this, eventData);
        }
    }

    private void raiseMessageRead(byte[] message) {
        MessageEvent eventData = new MessageEvent(message);
        dispatch(
                () -> {
                    for (MessageConnectionEventListener<MessageEvent> listener : messageReadListeners) {
                        listener.handle(this, eventData);
                    }
                },
                CancellationSignal.none());
    }

    private void raiseMessageWritten(byte[] message, CancellationSignal cancellationSignal) {
        MessageEvent eventData = new MessageEvent(message);
        dispatch(
                () -> {
                    for (MessageConnectionEventListener<MessageEvent> listener : messageWrittenListeners) {
                        listener.handle(this, eventData);
                    }
                },
                cancellationSignal);
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
    private void dispatch(Runnable event, CancellationSignal cancellationSignal) {
        event.run();
    }

    private static boolean isNullOrWhiteSpace(String value) {
        return CommonUtils.isNullOrWhiteSpace(value);
    }
}
