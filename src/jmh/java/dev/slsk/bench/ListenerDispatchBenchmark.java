// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.bench;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Defect 1.5: the framed read loop calls {@code addDataReadListener} and
 * {@code removeDataReadListener} around every payload, on a
 * {@link CopyOnWriteArrayList}.
 *
 * <p>Each add and each remove copies the whole backing array, so the cost is
 * two array copies per protocol message on the path that carries distributed
 * search traffic. This measures dispatch alone against dispatch plus the
 * add/remove churn, at listener counts of 0, 1 and 10.
 *
 * <p>The list is modelled rather than driven through {@code
 * DefaultMessageConnection} so the measurement isolates the collection cost
 * from socket I/O. The shape matches the production code exactly.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class ListenerDispatchBenchmark {

    /** Standing listeners, as a client would register. */
    @Param({"0", "1", "10"})
    public int listenerCount;

    private CopyOnWriteArrayList<Listener> listeners;
    private AtomicLong sink;
    private Listener transient1;

    interface Listener {
        void handle(long value);
    }

    @Setup
    public void setUp() {
        sink = new AtomicLong();
        listeners = new CopyOnWriteArrayList<>();
        for (int index = 0; index < listenerCount; index++) {
            listeners.add(value -> sink.addAndGet(value));
        }
        transient1 = value -> sink.addAndGet(value);
    }

    /** Dispatch only: what the loop would cost without the churn. */
    @Benchmark
    public void dispatchOnly(Blackhole blackhole) {
        for (Listener listener : listeners) {
            listener.handle(1);
        }
        blackhole.consume(listeners.size());
    }

    /**
     * Dispatch plus the per-message add/remove the read loop actually
     * performs. The delta against {@link #dispatchOnly} is the defect.
     */
    @Benchmark
    public void dispatchWithPerMessageChurn(Blackhole blackhole) {
        listeners.add(transient1);
        try {
            for (Listener listener : listeners) {
                listener.handle(1);
            }
            blackhole.consume(listeners.size());
        } finally {
            listeners.remove(transient1);
        }
    }
}
