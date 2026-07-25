// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.soak;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An in-process TCP peer bound to the loopback interface.
 *
 * <p>The soak harness never touches the real network and never needs
 * credentials. Every scenario runs against one of these, configured with the
 * {@link Behaviour} that the scenario needs.
 *
 * <p>The peer's own accept and service threads are virtual, so they do not
 * disturb the platform-thread census that several scenarios assert on.
 */
public final class LoopbackPeer implements AutoCloseable {

    /** What the peer does with each accepted connection. */
    public enum Behaviour {
        /** Accept and hold the socket open, exchanging nothing. */
        IDLE,
        /** Read and discard everything the client sends, forever. */
        SINK,
        /** Write framed protocol messages continuously until closed. */
        FRAME_FLOOD,
        /** Write a fixed number of raw bytes, then hold the socket open. */
        BYTE_SOURCE,
        /**
         * Accept and never read. The client's socket send buffer fills and its
         * writes stall, which is how the harness forces contention on the
         * connection write semaphore.
         */
        STALL
    }

    private final ServerSocket serverSocket;
    private final Behaviour behaviour;
    private final ExecutorService executor = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("soak-peer-", 0).factory());
    private final List<Socket> accepted = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong bytesRead = new AtomicLong();
    private final AtomicLong bytesWritten = new AtomicLong();
    private final AtomicLong framesWritten = new AtomicLong();
    private final CountDownLatch firstAccept = new CountDownLatch(1);

    private volatile long byteSourceLength;
    private volatile int framePayloadSize = 32;

    private LoopbackPeer(Behaviour behaviour, int backlog) throws IOException {
        this.behaviour = behaviour;
        this.serverSocket = new ServerSocket(0, backlog, InetAddress.getLoopbackAddress());
        executor.execute(this::acceptLoop);
    }

    /** Starts a peer with the given behaviour and a backlog sized for the scenario. */
    public static LoopbackPeer start(Behaviour behaviour, int backlog) throws IOException {
        return new LoopbackPeer(behaviour, backlog);
    }

    /** Starts a peer with a default backlog. */
    public static LoopbackPeer start(Behaviour behaviour) throws IOException {
        return new LoopbackPeer(behaviour, 256);
    }

    /** Sets how many bytes a {@link Behaviour#BYTE_SOURCE} peer serves per connection. */
    public LoopbackPeer withByteSourceLength(long length) {
        byteSourceLength = length;
        return this;
    }

    /** Sets the payload size of each frame a {@link Behaviour#FRAME_FLOOD} peer writes. */
    public LoopbackPeer withFramePayloadSize(int size) {
        framePayloadSize = size;
        return this;
    }

    /** Returns the endpoint clients should connect to. */
    public InetSocketAddress endpoint() {
        return new InetSocketAddress(serverSocket.getInetAddress(), serverSocket.getLocalPort());
    }

    /** Returns the bound port. */
    public int port() {
        return serverSocket.getLocalPort();
    }

    /** Returns the number of connections accepted so far. */
    public int acceptedCount() {
        return accepted.size();
    }

    /** Returns total bytes this peer has read from clients. */
    public long bytesRead() {
        return bytesRead.get();
    }

    /** Returns total bytes this peer has written to clients. */
    public long bytesWritten() {
        return bytesWritten.get();
    }

    /** Returns the number of protocol frames written by a flood peer. */
    public long framesWritten() {
        return framesWritten.get();
    }

    /** Blocks until at least one connection has been accepted. */
    public boolean awaitFirstAccept(long timeout, TimeUnit unit) throws InterruptedException {
        return firstAccept.await(timeout, unit);
    }

    /**
     * Abruptly closes every accepted socket without closing the listener.
     *
     * <p>Used to exercise half-open detection: the client sees a FIN it did not
     * initiate.
     */
    public void dropAllConnections() {
        for (Socket socket : accepted) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Best effort; the scenario asserts on the client's reaction.
            }
        }
        accepted.clear();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            serverSocket.close();
        } catch (IOException ignored) {
            // Nothing useful to do while tearing the harness down.
        }
        dropAllConnections();
        executor.shutdownNow();
    }

    private void acceptLoop() {
        while (!closed.get()) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                accepted.add(socket);
                firstAccept.countDown();
                executor.execute(() -> serve(socket));
            } catch (IOException exception) {
                if (closed.get()) {
                    return;
                }
            }
        }
    }

    private void serve(Socket socket) {
        try {
            switch (behaviour) {
                case IDLE -> holdOpen(socket);
                case SINK -> sink(socket);
                case FRAME_FLOOD -> floodFrames(socket);
                case BYTE_SOURCE -> serveBytes(socket);
                case STALL -> stall(socket);
            }
        } catch (IOException exception) {
            // A closed client socket is the normal end of every scenario.
        }
    }

    private void holdOpen(Socket socket) throws IOException {
        InputStream input = socket.getInputStream();
        byte[] buffer = new byte[1024];
        while (!closed.get() && !socket.isClosed()) {
            int read = input.read(buffer);
            if (read < 0) {
                return;
            }
            bytesRead.addAndGet(read);
        }
    }

    /**
     * Deliberately reads nothing. The socket stays open so the client does not
     * see a disconnect; its writes simply stop draining once the kernel buffers
     * fill.
     */
    private void stall(Socket socket) throws IOException {
        while (!closed.get() && !socket.isClosed()) {
            try {
                Thread.sleep(25);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void sink(Socket socket) throws IOException {
        InputStream input = socket.getInputStream();
        byte[] buffer = new byte[64 * 1024];
        while (!closed.get()) {
            int read = input.read(buffer);
            if (read < 0) {
                return;
            }
            bytesRead.addAndGet(read);
        }
    }

    private void floodFrames(Socket socket) throws IOException {
        OutputStream output = socket.getOutputStream();
        byte[] frame = buildFrame(framePayloadSize);
        while (!closed.get() && !socket.isClosed()) {
            output.write(frame);
            bytesWritten.addAndGet(frame.length);
            framesWritten.incrementAndGet();
        }
    }

    private void serveBytes(Socket socket) throws IOException {
        OutputStream output = socket.getOutputStream();
        byte[] buffer = new byte[64 * 1024];
        long remaining = byteSourceLength;
        while (remaining > 0 && !closed.get() && !socket.isClosed()) {
            int chunk = (int) Math.min(buffer.length, remaining);
            output.write(buffer, 0, chunk);
            bytesWritten.addAndGet(chunk);
            remaining -= chunk;
        }
        output.flush();
        holdOpen(socket);
    }

    /**
     * Builds one framed message: little-endian length, a four-byte code, then
     * payload. The length covers the code and the payload, matching the wire
     * format the connection read loop expects.
     */
    private static byte[] buildFrame(int payloadSize) {
        int codeLength = 4;
        byte[] frame = new byte[4 + codeLength + payloadSize];
        ByteBuffer.wrap(frame)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(codeLength + payloadSize)
                .putInt(1);
        return frame;
    }
}
