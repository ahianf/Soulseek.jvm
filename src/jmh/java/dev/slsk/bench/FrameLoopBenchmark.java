// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.bench;

import dev.slsk.CancellationSignal;
import dev.slsk.CancellationSubscription;
import dev.slsk.common.NetworkExecutor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Goal 2.1: thread amplification in the framed read path.
 *
 * <p>Reading one frame currently spans three virtual threads, two of them
 * parked. The loop's own thread calls {@code SocketConnection.readAsync}, which
 * dispatches to a fresh virtual thread and blocks on the future; that thread
 * calls {@code NetworkStream.readAsync}, which dispatches to another fresh
 * virtual thread and blocks on its future; only the third thread touches the
 * stream.
 *
 * <p>This benchmark reads frames from a fixed in-memory byte source through
 * three plumbing strategies over the library's real {@link NetworkExecutor}:
 * direct blocking, one level of dispatch, and the two levels production
 * actually uses. The source is in memory precisely so that the difference
 * between the three is plumbing and nothing else.
 *
 * <p>The plumbing is modelled rather than driven through {@code
 * SocketConnection}, which needs a socket. The shapes are copied from
 * {@code SocketConnection.async}, {@code SocketConnection.await} and
 * {@code NetworkStreamAdapter.observeCancellation}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class FrameLoopBenchmark {

    private static final int CODE_LENGTH = 4;
    private static final int PAYLOAD_SIZE = 32;

    private ExecutorService executor;
    private byte[] frame;

    @Setup
    public void setUp() {
        executor = NetworkExecutor.executor();
        frame = new byte[4 + CODE_LENGTH + PAYLOAD_SIZE];
        ByteBuffer.wrap(frame)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(CODE_LENGTH + PAYLOAD_SIZE)
                .putInt(1);
    }

    /** The floor: read the three fields straight off the stream. */
    @Benchmark
    public void directBlocking(Blackhole blackhole) throws Exception {
        InputStream source = new ByteArrayInputStream(frame);
        blackhole.consume(readDirect(source, 4));
        blackhole.consume(readDirect(source, CODE_LENGTH));
        blackhole.consume(readDirect(source, PAYLOAD_SIZE));
    }

    /** One dispatch per field, as {@code SocketConnection.readAsync} does. */
    @Benchmark
    public void singleDispatch(Blackhole blackhole) throws Exception {
        InputStream source = new ByteArrayInputStream(frame);
        blackhole.consume(await(async(() -> readDirect(source, 4))));
        blackhole.consume(await(async(() -> readDirect(source, CODE_LENGTH))));
        blackhole.consume(await(async(() -> readDirect(source, PAYLOAD_SIZE))));
    }

    /**
     * Two dispatches per field plus a cancellation registration, which is what
     * production does today. The delta against {@link #directBlocking} is the
     * cost Phase 3 removes.
     */
    @Benchmark
    public void productionShape(Blackhole blackhole) throws Exception {
        InputStream source = new ByteArrayInputStream(frame);
        blackhole.consume(await(async(() -> await(observed(() -> readDirect(source, 4))))));
        blackhole.consume(await(async(() -> await(observed(() -> readDirect(source, CODE_LENGTH))))));
        blackhole.consume(await(async(() -> await(observed(() -> readDirect(source, PAYLOAD_SIZE))))));
    }

    private static byte[] readDirect(InputStream source, int length) throws IOException {
        byte[] buffer = new byte[length];
        int read = 0;
        while (read < length) {
            int count = source.read(buffer, read, length - read);
            if (count < 0) {
                break;
            }
            read += count;
        }
        return buffer;
    }

    /** Mirrors {@code SocketConnection.async}. */
    private <T> CompletableFuture<T> async(Callable<T> callable) {
        CompletableFuture<T> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                future.complete(callable.call());
            } catch (Throwable exception) {
                future.completeExceptionally(exception);
            }
        });
        return future;
    }

    /**
     * Mirrors {@code NetworkStreamAdapter.readAsync} plus
     * {@code observeCancellation}: a second dispatch wrapped in a cancellation
     * registration against the shared non-cancellable singleton.
     */
    private <T> CompletableFuture<T> observed(Callable<T> callable) {
        CompletableFuture<T> operation = async(callable);
        CancellationSignal token = CancellationSignal.none();
        CancellationSubscription registration = token.register(() -> operation.cancel(false));
        operation.whenComplete((ignored, exception) -> registration.close());
        return operation;
    }

    /** Mirrors {@code SocketConnection.await}. */
    private static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get();
    }
}
