// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.search.FileAttributes;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Every fixture is synthesized from the format specification, the same way the
 * codec tests build wire bytes: what is asserted is the header arithmetic, not
 * a binary blob's provenance.
 */
class AudioMetadataTest {

    @TempDir
    Path directory;

    // ---- FLAC --------------------------------------------------------------

    @Test
    void flacStreamInfoYieldsDurationSampleRateAndBitDepth() throws IOException {
        Path file = write("song.flac", flac(44100, 16, 44100L * 185));
        FileAttributes attributes = AudioMetadata.probe(file);

        assertEquals(Duration.ofSeconds(185), attributes.duration().orElseThrow());
        assertEquals(44100, attributes.sampleRate().orElseThrow());
        assertEquals(16, attributes.bitDepth().orElseThrow());
        assertTrue(attributes.bitrate().isEmpty(), "lossless files do not advertise a bitrate");
    }

    @Test
    void flacWithLeadingId3TagStillParses() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.writeBytes(id3v2(500));
        bytes.writeBytes(new byte[500]);
        bytes.writeBytes(flac(96000, 24, 96000L * 30));
        FileAttributes attributes = AudioMetadata.probe(write("tagged.flac", bytes.toByteArray()));

        assertEquals(Duration.ofSeconds(30), attributes.duration().orElseThrow());
        assertEquals(96000, attributes.sampleRate().orElseThrow());
        assertEquals(24, attributes.bitDepth().orElseThrow());
    }

    @Test
    void flacWithUnknownSampleCountOmitsDuration() throws IOException {
        FileAttributes attributes = AudioMetadata.probe(write("stream.flac", flac(44100, 16, 0)));

        assertTrue(attributes.duration().isEmpty());
        assertEquals(44100, attributes.sampleRate().orElseThrow());
    }

    // ---- WAV ---------------------------------------------------------------

    @Test
    void wavFmtAndDataChunksYieldTheLosslessSet() throws IOException {
        FileAttributes attributes = AudioMetadata.probe(write("take.wav", wav(48000, 2, 24, 61)));

        assertEquals(Duration.ofSeconds(61), attributes.duration().orElseThrow());
        assertEquals(48000, attributes.sampleRate().orElseThrow());
        assertEquals(24, attributes.bitDepth().orElseThrow());
        assertTrue(attributes.bitrate().isEmpty());
    }

    // ---- MP3 ---------------------------------------------------------------

    @Test
    void constantBitrateMp3IsTimedByItsSize() throws IOException {
        // 192 kbps for 30 seconds is 720,000 bytes of audio.
        byte[] audio = new byte[720_000];
        int frameLength = 144 * 192_000 / 44100;
        writeMp3FrameHeader(audio, 0, 11);
        writeMp3FrameHeader(audio, frameLength, 11);
        FileAttributes attributes = AudioMetadata.probe(write("cbr.mp3", audio));

        assertEquals(192, attributes.bitrate().orElseThrow());
        assertEquals(Duration.ofSeconds(30), attributes.duration().orElseThrow());
        assertFalse(attributes.variableBitRate());
    }

    @Test
    void id3TagIsSkippedAndDoesNotCountTowardDuration() throws IOException {
        byte[] audio = new byte[720_000];
        int frameLength = 144 * 192_000 / 44100;
        writeMp3FrameHeader(audio, 0, 11);
        writeMp3FrameHeader(audio, frameLength, 11);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.writeBytes(id3v2(4000));
        bytes.writeBytes(new byte[4000]);
        bytes.writeBytes(audio);
        FileAttributes attributes = AudioMetadata.probe(write("tagged.mp3", bytes.toByteArray()));

        assertEquals(192, attributes.bitrate().orElseThrow());
        assertEquals(Duration.ofSeconds(30), attributes.duration().orElseThrow());
    }

    @Test
    void xingHeaderYieldsVbrDurationAndAverageBitrate() throws IOException {
        // 7,675 frames of 1,152 samples at 44,100 Hz is 200.4 seconds; five
        // million bytes over that is a 200 kbps average.
        byte[] audio = new byte[8192];
        int frameLength = 144 * 128_000 / 44100;
        writeMp3FrameHeader(audio, 0, 9);
        writeMp3FrameHeader(audio, frameLength, 9);
        putAscii(audio, 36, "Xing");
        putInt(audio, 40, 0x3);
        putInt(audio, 44, 7_675);
        putInt(audio, 48, 5_000_000);
        FileAttributes attributes = AudioMetadata.probe(write("vbr.mp3", audio));

        assertTrue(attributes.variableBitRate());
        assertEquals(Duration.ofSeconds(200), attributes.duration().orElseThrow());
        assertEquals(200, attributes.bitrate().orElseThrow());
    }

    @Test
    void infoHeaderIsCbrWithItsFrameCountDuration() throws IOException {
        byte[] audio = new byte[8192];
        int frameLength = 144 * 128_000 / 44100;
        writeMp3FrameHeader(audio, 0, 9);
        writeMp3FrameHeader(audio, frameLength, 9);
        putAscii(audio, 36, "Info");
        putInt(audio, 40, 0x1);
        putInt(audio, 44, 3_830);
        FileAttributes attributes = AudioMetadata.probe(write("info.mp3", audio));

        assertFalse(attributes.variableBitRate());
        assertEquals(Duration.ofSeconds(100), attributes.duration().orElseThrow());
        assertEquals(128, attributes.bitrate().orElseThrow());
    }

    @Test
    void aFalseSyncInGarbageIsNotAFrame() throws IOException {
        byte[] audio = new byte[4096];
        audio[100] = (byte) 0xFF;
        audio[101] = (byte) 0xFB;
        // An invalid bitrate index right after a plausible sync.
        audio[102] = (byte) 0xF0;
        FileAttributes attributes = AudioMetadata.probe(write("noise.mp3", audio));

        assertEquals(FileAttributes.none(), attributes);
    }

    // ---- Ogg ---------------------------------------------------------------

    @Test
    void oggVorbisYieldsNominalBitrateAndGranuleDuration() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.writeBytes(oggPage(0x02, 0, vorbisIdentification(44100, 160_000)));
        bytes.writeBytes(oggPage(0x04, 44100L * 95, new byte[0]));
        FileAttributes attributes = AudioMetadata.probe(write("song.ogg", bytes.toByteArray()));

        assertEquals(160, attributes.bitrate().orElseThrow());
        assertEquals(Duration.ofSeconds(95), attributes.duration().orElseThrow());
        assertTrue(attributes.variableBitRate(), "a nominal-only Vorbis stream is variable");
    }

    @Test
    void opusDurationComesFromTheGranuleLessPreSkip() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.writeBytes(oggPage(0x02, 0, opusHead(312)));
        bytes.writeBytes(oggPage(0x04, 312 + 48000L * 140, new byte[0]));
        FileAttributes attributes = AudioMetadata.probe(write("voice.opus", bytes.toByteArray()));

        assertEquals(Duration.ofSeconds(140), attributes.duration().orElseThrow());
        assertTrue(attributes.variableBitRate());
        assertTrue(attributes.bitrate().isEmpty(), "a bitrate this file is too small to have is not published");
    }

    // ---- MP4 ---------------------------------------------------------------

    @Test
    void m4aAacYieldsEsdsBitrateAndMvhdDuration() throws IOException {
        byte[] esds = box("esds", esdsDescriptor(128_000));
        byte[] entry = sampleEntry("mp4a", 44100, 16, esds);
        byte[] file = concat(
                box("ftyp", "M4A mp42".getBytes(StandardCharsets.US_ASCII)),
                box(
                        "moov",
                        concat(
                                box("mvhd", mvhd(600, 600L * 74)),
                                box("trak", box("mdia", box("minf", box("stbl", box("stsd", stsd(entry)))))))));
        FileAttributes attributes = AudioMetadata.probe(write("song.m4a", file));

        assertEquals(128, attributes.bitrate().orElseThrow());
        assertEquals(Duration.ofSeconds(74), attributes.duration().orElseThrow());
        assertFalse(attributes.variableBitRate());
    }

    @Test
    void m4aAlacYieldsTheCookiesSampleRateAndBitDepth() throws IOException {
        byte[] cookie = box("alac", alacCookie(24, 96000));
        byte[] entry = sampleEntry("alac", 44100, 16, cookie);
        byte[] file = concat(
                box("ftyp", "M4A mp42".getBytes(StandardCharsets.US_ASCII)),
                box(
                        "moov",
                        concat(
                                box("mvhd", mvhd(600, 600L * 42)),
                                box("trak", box("mdia", box("minf", box("stbl", box("stsd", stsd(entry)))))))));
        FileAttributes attributes = AudioMetadata.probe(write("song.m4a", file));

        assertEquals(Duration.ofSeconds(42), attributes.duration().orElseThrow());
        assertEquals(96000, attributes.sampleRate().orElseThrow());
        assertEquals(24, attributes.bitDepth().orElseThrow());
        assertTrue(attributes.bitrate().isEmpty());
    }

    // ---- refusal -----------------------------------------------------------

    @Test
    void unrecognizedTruncatedAndMissingFilesYieldNone() throws IOException {
        assertEquals(FileAttributes.none(), AudioMetadata.probe(write("notes.txt", new byte[100])));
        assertEquals(FileAttributes.none(), AudioMetadata.probe(write("empty.flac", new byte[0])));
        assertEquals(FileAttributes.none(), AudioMetadata.probe(write("short.flac", new byte[] {'f', 'L', 'a'})));
        assertEquals(FileAttributes.none(), AudioMetadata.probe(write("noext", new byte[100])));
        assertEquals(FileAttributes.none(), AudioMetadata.probe(directory.resolve("missing.mp3")));
        assertEquals(FileAttributes.none(), AudioMetadata.probe(write("garbage.ogg", new byte[64])));
        assertEquals(FileAttributes.none(), AudioMetadata.probe(write("garbage.m4a", new byte[64])));
        assertEquals(FileAttributes.none(), AudioMetadata.probe(write("garbage.wav", new byte[64])));
    }

    // ---- fixture builders --------------------------------------------------

    private Path write(String name, byte[] bytes) throws IOException {
        Path file = directory.resolve(name);
        Files.write(file, bytes);
        return file;
    }

    private static byte[] flac(int sampleRate, int bitDepth, long totalSamples) {
        byte[] bytes = new byte[4 + 4 + 34];
        putAscii(bytes, 0, "fLaC");
        bytes[4] = (byte) 0x80; // last block, STREAMINFO
        bytes[7] = 34;
        int at = 8;
        // Block sizes and frame sizes are irrelevant to the probe.
        bytes[at + 10] = (byte) (sampleRate >> 12);
        bytes[at + 11] = (byte) (sampleRate >> 4);
        bytes[at + 12] = (byte) (((sampleRate & 0xF) << 4) | (1 << 1) | ((bitDepth - 1) >> 4));
        bytes[at + 13] = (byte) (((bitDepth - 1) << 4) | (int) ((totalSamples >> 32) & 0xF));
        bytes[at + 14] = (byte) (totalSamples >> 24);
        bytes[at + 15] = (byte) (totalSamples >> 16);
        bytes[at + 16] = (byte) (totalSamples >> 8);
        bytes[at + 17] = (byte) totalSamples;
        return bytes;
    }

    private static byte[] wav(int sampleRate, int channels, int bitDepth, int seconds) {
        int byteRate = sampleRate * channels * bitDepth / 8;
        ByteBuffer buffer = ByteBuffer.allocate(12 + 8 + 16 + 8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put("RIFF".getBytes(StandardCharsets.US_ASCII)).putInt(0);
        buffer.put("WAVE".getBytes(StandardCharsets.US_ASCII));
        buffer.put("fmt ".getBytes(StandardCharsets.US_ASCII)).putInt(16);
        buffer.putShort((short) 1)
                .putShort((short) channels)
                .putInt(sampleRate)
                .putInt(byteRate)
                .putShort((short) (channels * bitDepth / 8))
                .putShort((short) bitDepth);
        buffer.put("data".getBytes(StandardCharsets.US_ASCII)).putInt(byteRate * seconds);
        // The data itself is not read; the declared size is what times the file.
        return buffer.array();
    }

    /** Writes an MPEG-1 Layer III, 44.1 kHz, joint-stereo frame header. */
    private static void writeMp3FrameHeader(byte[] bytes, int at, int bitrateIndex) {
        bytes[at] = (byte) 0xFF;
        bytes[at + 1] = (byte) 0xFB;
        bytes[at + 2] = (byte) (bitrateIndex << 4);
        bytes[at + 3] = (byte) 0x40;
    }

    private static byte[] id3v2(int size) {
        byte[] bytes = new byte[10];
        putAscii(bytes, 0, "ID3");
        bytes[3] = 4;
        bytes[6] = (byte) ((size >> 21) & 0x7F);
        bytes[7] = (byte) ((size >> 14) & 0x7F);
        bytes[8] = (byte) ((size >> 7) & 0x7F);
        bytes[9] = (byte) (size & 0x7F);
        return bytes;
    }

    private static byte[] oggPage(int type, long granule, byte[] packet) {
        int segments = Math.max(1, (packet.length + 254) / 255);
        ByteBuffer buffer = ByteBuffer.allocate(27 + segments + packet.length).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put("OggS".getBytes(StandardCharsets.US_ASCII));
        buffer.put((byte) 0)
                .put((byte) type)
                .putLong(granule)
                .putInt(1)
                .putInt(0)
                .putInt(0);
        buffer.put((byte) segments);
        int remaining = packet.length;
        for (int segment = 0; segment < segments; segment++) {
            buffer.put((byte) Math.min(255, remaining));
            remaining -= Math.min(255, remaining);
        }
        buffer.put(packet);
        return buffer.array();
    }

    private static byte[] vorbisIdentification(int sampleRate, int nominalBitrate) {
        ByteBuffer buffer = ByteBuffer.allocate(30).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1).put("vorbis".getBytes(StandardCharsets.US_ASCII));
        buffer.putInt(0).put((byte) 2).putInt(sampleRate);
        buffer.putInt(0).putInt(nominalBitrate).putInt(0);
        return buffer.array();
    }

    private static byte[] opusHead(int preSkip) {
        ByteBuffer buffer = ByteBuffer.allocate(19).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put("OpusHead".getBytes(StandardCharsets.US_ASCII));
        buffer.put((byte) 1).put((byte) 2).putShort((short) preSkip).putInt(48000);
        buffer.putShort((short) 0).put((byte) 0);
        return buffer.array();
    }

    private static byte[] mvhd(int timescale, long duration) {
        ByteBuffer buffer = ByteBuffer.allocate(24);
        buffer.putInt(0); // version and flags
        buffer.putInt(0).putInt(0); // creation and modification time
        buffer.putInt(timescale).putInt((int) duration);
        return buffer.array();
    }

    private static byte[] stsd(byte[] entry) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + entry.length);
        buffer.putInt(0).putInt(1).put(entry);
        return buffer.array();
    }

    private static byte[] sampleEntry(String format, int sampleRate, int sampleSize, byte[] child) {
        ByteBuffer buffer = ByteBuffer.allocate(36 + child.length);
        buffer.putInt(36 + child.length);
        buffer.put(format.getBytes(StandardCharsets.US_ASCII));
        buffer.put(new byte[6]).putShort((short) 1); // reserved, data reference
        buffer.putShort((short) 0).putShort((short) 0).putInt(0); // version, revision, vendor
        buffer.putShort((short) 2).putShort((short) sampleSize);
        buffer.putShort((short) 0).putShort((short) 0); // compression, packet size
        buffer.putInt(sampleRate << 16);
        buffer.put(child);
        return buffer.array();
    }

    private static byte[] esdsDescriptor(int averageBitrate) {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 2 + 3 + 2 + 1 + 4 + 4 + 4);
        buffer.putInt(0); // version and flags
        buffer.put((byte) 0x03).put((byte) 0x12);
        buffer.putShort((short) 0).put((byte) 0); // ES id and flags
        buffer.put((byte) 0x04).put((byte) 0x0D);
        buffer.put((byte) 0x40); // AAC object type
        buffer.putInt(0); // stream type and buffer size
        buffer.putInt(256_000).putInt(averageBitrate);
        return buffer.array();
    }

    private static byte[] alacCookie(int bitDepth, int sampleRate) {
        ByteBuffer buffer = ByteBuffer.allocate(28);
        buffer.putInt(0); // version and flags
        buffer.putInt(4096).put((byte) 0).put((byte) bitDepth);
        buffer.put((byte) 40).put((byte) 10).put((byte) 14).put((byte) 2);
        buffer.putShort((short) 255).putInt(0).putInt(0);
        buffer.putInt(sampleRate);
        return buffer.array();
    }

    private static byte[] box(String type, byte[] payload) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + payload.length);
        buffer.putInt(8 + payload.length);
        buffer.put(type.getBytes(StandardCharsets.US_ASCII));
        buffer.put(payload);
        return buffer.array();
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            bytes.writeBytes(part);
        }
        return bytes.toByteArray();
    }

    private static void putAscii(byte[] bytes, int at, String text) {
        for (int index = 0; index < text.length(); index++) {
            bytes[at + index] = (byte) text.charAt(index);
        }
    }

    private static void putInt(byte[] bytes, int at, int value) {
        bytes[at] = (byte) (value >> 24);
        bytes[at + 1] = (byte) (value >> 16);
        bytes[at + 2] = (byte) (value >> 8);
        bytes[at + 3] = (byte) value;
    }
}
