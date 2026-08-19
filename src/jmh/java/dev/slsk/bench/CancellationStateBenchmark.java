// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.bench;

import dev.slsk.internal.concurrent.CancellationController;
import dev.slsk.internal.concurrent.CancellationSignal;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Defect 1.1: {@code CancellationSignal.none()} is a process-wide singleton
 * whose {@code isCancellationRequested()} is {@code synchronized}.
 *
 * <p>{@code SocketConnection.readInternal} calls it three times per buffer
 * chunk and {@code NetworkStreamAdapter} adds two or three more, so this one
 * monitor is taken roughly six times per 16 KiB by every connection in the
 * process. The contended variants below are the measurement that matters; the
 * single-threaded ones establish the uncontended floor.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class CancellationStateBenchmark {

    private CancellationSignal none;
    private CancellationSignal live;
    private CancellationController controller;

    @Setup
    public void setUp() {
        none = CancellationSignal.none();
        controller = new CancellationController();
        live = controller.getSignal();
    }

    /** The uncontended floor for the non-cancellable singleton. */
    @Benchmark
    @Threads(1)
    public boolean noneSingleThreaded() {
        return none.isCancellationRequested();
    }

    /** The shared-monitor cost that every connection actually pays. */
    @Benchmark
    @Threads(8)
    public boolean noneEightThreads() {
        return none.isCancellationRequested();
    }

    /** Scaling behaviour past core count, where monitor queuing dominates. */
    @Benchmark
    @Threads(32)
    public boolean noneThirtyTwoThreads() {
        return none.isCancellationRequested();
    }

    /**
     * The read loop's real call shape: three checks per chunk against the
     * shared singleton.
     */
    @Benchmark
    @Threads(8)
    public void readChunkCheckPattern(Blackhole blackhole) {
        blackhole.consume(none.isCancellationRequested());
        blackhole.consume(none.isCancellationRequested());
        blackhole.consume(none.isCancellationRequested());
    }

    /** A per-instance signal, for contrast with the shared singleton. */
    @Benchmark
    @Threads(8)
    public boolean liveSignalEightThreads() {
        return live.isCancellationRequested();
    }

    /**
     * Register and release against the non-cancellable singleton, which
     * {@code NetworkStreamAdapter} does around every stream read.
     */
    @Benchmark
    @Threads(8)
    public void registerAndCloseOnNone() {
        none.register(() -> {}).close();
    }
}
