// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.bench;

import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
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
 * Field decoding across representative message shapes.
 *
 * <p>The codecs are explicitly out of scope for redesign — they are on the
 * non-goals list — so this benchmark is a regression guard rather than an
 * improvement target. It exists so that a change made elsewhere in the fork
 * cannot silently slow down parsing, and so the allocation-per-message figure
 * from the soak harness can be attributed between parsing and the async
 * machinery around it.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class MessageReaderBenchmark {

    private byte[] scalarMessage;
    private byte[] stringMessage;
    private byte[] mixedMessage;

    @Setup
    public void setUp() {
        scalarMessage = new MessageBuilder()
                .writeCode(MessageCode.Server.LOGIN)
                .writeInteger(1)
                .writeInteger(2)
                .writeInteger(3)
                .writeInteger(4)
                .build();

        stringMessage = new MessageBuilder()
                .writeCode(MessageCode.Server.LOGIN)
                .writeString("a-representative-soulseek-username")
                .writeString("a-representative-room-name")
                .writeString("some longer free text of the kind a private message carries")
                .build();

        mixedMessage = new MessageBuilder()
                .writeCode(MessageCode.Server.LOGIN)
                .writeInteger(42)
                .writeString("username")
                .writeLong(1234567890123L)
                .writeString("\\\\path\\\\to\\\\a\\\\remote\\\\file.flac")
                .writeInteger(7)
                .writeByte(1)
                .build();
    }

    /** Four little-endian integers: the cheapest realistic shape. */
    @Benchmark
    public void readScalars(Blackhole blackhole) {
        MessageReader<MessageCode.Server> reader = new MessageReader<>(scalarMessage, MessageCode.Server.class);
        blackhole.consume(reader.readCode());
        blackhole.consume(reader.readInteger());
        blackhole.consume(reader.readInteger());
        blackhole.consume(reader.readInteger());
        blackhole.consume(reader.readInteger());
    }

    /** Three length-prefixed strings, which dominate real server traffic. */
    @Benchmark
    public void readStrings(Blackhole blackhole) {
        MessageReader<MessageCode.Server> reader = new MessageReader<>(stringMessage, MessageCode.Server.class);
        blackhole.consume(reader.readCode());
        blackhole.consume(reader.readString());
        blackhole.consume(reader.readString());
        blackhole.consume(reader.readString());
    }

    /** A mixed shape closer to a real peer response. */
    @Benchmark
    public void readMixed(Blackhole blackhole) {
        MessageReader<MessageCode.Server> reader = new MessageReader<>(mixedMessage, MessageCode.Server.class);
        blackhole.consume(reader.readCode());
        blackhole.consume(reader.readInteger());
        blackhole.consume(reader.readString());
        blackhole.consume(reader.readLong());
        blackhole.consume(reader.readString());
        blackhole.consume(reader.readInteger());
        blackhole.consume(reader.readByte());
    }

    /**
     * Construction alone. The constructor copies the payload out of the frame
     * with {@code Arrays.copyOfRange}, so this is the per-message allocation
     * floor before any field is read.
     */
    @Benchmark
    public MessageReader<MessageCode.Server> constructReader() {
        return new MessageReader<>(mixedMessage, MessageCode.Server.class);
    }
}
