// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import dev.slsk.CancellationSignal;
import dev.slsk.CancellationSubscription;
import dev.slsk.common.NetworkExecutor;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Pass-through implementation of {@link NetworkStream} over a socket.
 */
final class NetworkStreamAdapter implements NetworkStream {
    private final Socket socket;
    private final InputStream inputStream;
    private final OutputStream outputStream;
    private int writeTimeout = -1;

    NetworkStreamAdapter(Socket socket) throws IOException {
        this.socket = Objects.requireNonNull(socket, "socket");
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
    }

    @Override
    public int getReadTimeout() throws IOException {
        int timeout = socket.getSoTimeout();
        return timeout == 0 ? -1 : timeout;
    }

    @Override
    public void setReadTimeout(int timeout) throws IOException {
        validateTimeout(timeout);
        socket.setSoTimeout(timeout == -1 ? 0 : timeout);
    }

    @Override
    public int getWriteTimeout() {
        return writeTimeout;
    }

    @Override
    public void setWriteTimeout(int timeout) {
        validateTimeout(timeout);
        writeTimeout = timeout;
    }

    @Override
    public CompletableFuture<Integer> readAsync(
            byte[] buffer, int offset, int size, CancellationSignal cancellationSignal) {
        Objects.requireNonNull(buffer, "buffer");
        Objects.checkFromIndexSize(offset, size, buffer.length);
        CancellationSignal token = Objects.requireNonNull(cancellationSignal, "cancellationSignal");
        if (token.isCancellationRequested()) {
            return cancelledFuture();
        }
        return observeCancellation(
                NetworkExecutor.supplyAsync(() -> {
                    token.throwIfCancellationRequested();
                    try {
                        int bytesRead = inputStream.read(buffer, offset, size);
                        token.throwIfCancellationRequested();
                        return bytesRead < 0 ? 0 : bytesRead;
                    } catch (IOException exception) {
                        throw new CompletionException(exception);
                    }
                }),
                token);
    }

    @Override
    public CompletableFuture<Void> writeAsync(
            byte[] buffer, int offset, int size, CancellationSignal cancellationSignal) {
        Objects.requireNonNull(buffer, "buffer");
        Objects.checkFromIndexSize(offset, size, buffer.length);
        CancellationSignal token = Objects.requireNonNull(cancellationSignal, "cancellationSignal");
        if (token.isCancellationRequested()) {
            return cancelledFuture();
        }
        return observeCancellation(
                NetworkExecutor.runAsync(() -> {
                    token.throwIfCancellationRequested();
                    try {
                        outputStream.write(buffer, offset, size);
                        token.throwIfCancellationRequested();
                    } catch (IOException exception) {
                        throw new CompletionException(exception);
                    }
                }),
                token);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    private static void validateTimeout(int timeout) {
        if (timeout <= 0 && timeout != -1) {
            throw new IllegalArgumentException("Timeout must be positive or -1");
        }
    }

    /**
     * Observes cancellation without touching the socket.
     *
     * <p>This used to close the socket, because a blocking stream call cannot
     * be aborted from another thread and the port needed cancellation to take
     * effect promptly. The cost was that cancelling one transfer tore down the
     * connection carrying it.
     *
     * <p>Interrupting the reader is not an alternative: interrupting a virtual
     * thread blocked in {@code java.net.Socket} read closes the socket too — the
     * JDK throws {@code SocketException: Closed by interrupt} and the socket is
     * unusable afterwards. Measured, not assumed.
     *
     * <p>What does work is a bounded {@code SO_TIMEOUT}. The read returns
     * {@link java.net.SocketTimeoutException} periodically with the socket
     * intact and no bytes lost, and {@code SocketConnection.readInternal} uses
     * that as its cancellation check point. So this method now only marks the
     * future; the loop above it does the aborting.
     */
    private <T> CompletableFuture<T> observeCancellation(CompletableFuture<T> operation, CancellationSignal token) {
        CancellationSubscription registration = token.register(() -> operation.cancel(false));
        operation.whenComplete((ignored, exception) -> registration.close());
        return operation;
    }

    private static <T> CompletableFuture<T> cancelledFuture() {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.cancel(false);
        return future;
    }
}
