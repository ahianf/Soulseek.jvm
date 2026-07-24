// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.CancellationToken;
import dev.slsk.common.CommonUtils;
import dev.slsk.common.EventDispatch;
import dev.slsk.exceptions.MessageException;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.network.tcp.ConnectionDataEventArgs;
import dev.slsk.network.tcp.ConnectionEventListener;
import dev.slsk.network.tcp.ConnectionKey;
import dev.slsk.network.tcp.SocketConnection;
import dev.slsk.network.tcp.TcpClient;
import dev.slsk.options.ConnectionOptions;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/** Provides framed client connections to the Soulseek network. */
public final class DefaultMessageConnection extends SocketConnection implements MessageConnection {

    private final CopyOnWriteArrayList<MessageConnectionEventListener<MessageDataEventArgs>> messageDataReadListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<MessageConnectionEventListener<MessageEventArgs>> messageReadListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<MessageConnectionEventListener<MessageReceivedEventArgs>>
            messageReceivedListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<MessageConnectionEventListener<MessageEventArgs>> messageWrittenListeners =
            new CopyOnWriteArrayList<>();

    private final int codeLength;
    private final String username;
    private volatile boolean readingContinuously;

    /** Creates a server connection with source defaults. */
    public DefaultMessageConnection(InetSocketAddress ipEndPoint) {
        this(ipEndPoint, null, 4, null);
    }

    /** Creates a server connection. */
    public DefaultMessageConnection(
            InetSocketAddress ipEndPoint, ConnectionOptions options, int codeLength, TcpClient tcpClient) {
        super(ipEndPoint, options, tcpClient);
        this.codeLength = codeLength;
        username = "";
        bindConnectedReadLoop();
    }

    /** Creates a peer connection with source defaults. */
    public DefaultMessageConnection(String username, InetSocketAddress ipEndPoint) {
        this(username, ipEndPoint, null, 4, null);
    }

    /** Creates a peer connection. */
    public DefaultMessageConnection(
            String username,
            InetSocketAddress ipEndPoint,
            ConnectionOptions options,
            int codeLength,
            TcpClient tcpClient) {
        super(ipEndPoint, options, tcpClient);
        this.codeLength = codeLength;
        if (isNullOrWhiteSpace(username)) {
            throw new IllegalArgumentException(
                    "The username must not be a null or empty string, " + "or one consisting only of whitespace");
        }
        this.username = username;
        bindConnectedReadLoop();
    }

    @Override
    public void addMessageDataReadListener(MessageConnectionEventListener<MessageDataEventArgs> listener) {
        messageDataReadListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeMessageDataReadListener(MessageConnectionEventListener<MessageDataEventArgs> listener) {
        messageDataReadListeners.remove(listener);
    }

    @Override
    public void addMessageReadListener(MessageConnectionEventListener<MessageEventArgs> listener) {
        messageReadListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeMessageReadListener(MessageConnectionEventListener<MessageEventArgs> listener) {
        messageReadListeners.remove(listener);
    }

    @Override
    public void addMessageReceivedListener(MessageConnectionEventListener<MessageReceivedEventArgs> listener) {
        messageReceivedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeMessageReceivedListener(MessageConnectionEventListener<MessageReceivedEventArgs> listener) {
        messageReceivedListeners.remove(listener);
    }

    @Override
    public void addMessageWrittenListener(MessageConnectionEventListener<MessageEventArgs> listener) {
        messageWrittenListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeMessageWrittenListener(MessageConnectionEventListener<MessageEventArgs> listener) {
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
        return new ConnectionKey(username, getIpEndPoint());
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
            CommonUtils.forget(readContinuouslyAsync());
        }
    }

    @Override
    public CompletableFuture<Void> writeAsync(OutgoingMessage message, CancellationToken cancellationToken) {
        if (message == null) {
            throw new IllegalArgumentException("The specified message is null");
        }
        byte[] bytes;
        try {
            bytes = message.toByteArray();
        } catch (Exception exception) {
            throw new MessageException("Failed to convert the message to a byte array", exception);
        }
        CancellationToken token = cancellationToken == null ? CancellationToken.none() : cancellationToken;
        return super.writeAsync(bytes, token).thenRun(() -> raiseMessageWritten(bytes, token));
    }

    CompletableFuture<Void> readContinuouslyAsync() {
        synchronized (this) {
            if (readingContinuously) {
                return CompletableFuture.completedFuture(null);
            }
            readingContinuously = true;
        }
        return CompletableFuture.runAsync(() -> {
            byte[][] codeHolder = new byte[1][];
            ConnectionEventListener<ConnectionDataEventArgs> payloadProgress = (sender, args) ->
                    raiseMessageDataRead(codeHolder[0], args.getCurrentLength(), args.getTotalLength());
            try {
                while (!isDisposed()) {
                    ByteArrayOutputStream message = new ByteArrayOutputStream();
                    try {
                        byte[] lengthBytes =
                                readAsync(4, CancellationToken.none()).join();
                        int length = ByteBuffer.wrap(lengthBytes)
                                .order(ByteOrder.LITTLE_ENDIAN)
                                .getInt();
                        message.writeBytes(lengthBytes);

                        byte[] codeBytes =
                                readAsync(codeLength, CancellationToken.none()).join();
                        codeHolder[0] = codeBytes;
                        message.writeBytes(codeBytes);

                        raiseMessageDataRead(codeBytes, 0, length - codeLength);
                        raiseMessageReceived(length, codeBytes);

                        addDataReadListener(payloadProgress);
                        byte[] payload = readAsync(length - codeLength, CancellationToken.none())
                                .join();
                        message.writeBytes(payload);
                        raiseMessageRead(message.toByteArray());
                    } finally {
                        removeDataReadListener(payloadProgress);
                    }
                }
            } finally {
                readingContinuously = false;
            }
        });
    }

    private void bindConnectedReadLoop() {
        addConnectedListener((sender, args) -> CommonUtils.forget(readContinuouslyAsync()));
    }

    private void raiseMessageDataRead(byte[] code, long currentLength, long totalLength) {
        MessageDataEventArgs eventArgs = new MessageDataEventArgs(code, currentLength, totalLength);
        dispatch(
                () -> {
                    for (MessageConnectionEventListener<MessageDataEventArgs> listener : messageDataReadListeners) {
                        listener.handle(this, eventArgs);
                    }
                },
                CancellationToken.none());
    }

    private void raiseMessageReceived(long length, byte[] code) {
        MessageReceivedEventArgs eventArgs = new MessageReceivedEventArgs(length, code);
        for (MessageConnectionEventListener<MessageReceivedEventArgs> listener : messageReceivedListeners) {
            listener.handle(this, eventArgs);
        }
    }

    private void raiseMessageRead(byte[] message) {
        MessageEventArgs eventArgs = new MessageEventArgs(message);
        dispatch(
                () -> {
                    for (MessageConnectionEventListener<MessageEventArgs> listener : messageReadListeners) {
                        listener.handle(this, eventArgs);
                    }
                },
                CancellationToken.none());
    }

    private void raiseMessageWritten(byte[] message, CancellationToken cancellationToken) {
        MessageEventArgs eventArgs = new MessageEventArgs(message);
        dispatch(
                () -> {
                    for (MessageConnectionEventListener<MessageEventArgs> listener : messageWrittenListeners) {
                        listener.handle(this, eventArgs);
                    }
                },
                cancellationToken);
    }

    private static void dispatch(Runnable event, CancellationToken cancellationToken) {
        if (EventDispatch.isAsynchronous()) {
            if (!cancellationToken.isCancellationRequested()) {
                CompletableFuture.runAsync(event);
            }
        } else {
            event.run();
        }
    }

    private static boolean isNullOrWhiteSpace(String value) {
        return value == null
                || value.isEmpty()
                || value.codePoints()
                        .allMatch(codePoint -> Character.isWhitespace(codePoint) || Character.isSpaceChar(codePoint));
    }
}
