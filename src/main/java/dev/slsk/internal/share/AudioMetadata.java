// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-FileCopyrightText: 2014-2025 Tom Wallroth, Mat (mathiascode) and tinytag contributors
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import dev.slsk.search.FileAttributeType;
import dev.slsk.search.FileAttributes;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Reads the audio attributes peers are shown: bitrate, duration, sample rate,
 * bit depth and the VBR flag, from the file's own headers.
 *
 * <p>The parsing is a Java port of the header logic in tinytag (MIT), the
 * library Nicotine+ vendors for exactly this purpose — tags and pictures are
 * not read, only the stream headers, so a probe costs a few small reads
 * regardless of file size.
 *
 * <p>What is published follows the convention the protocol document records
 * for modern clients: lossy formats carry {@code {bitrate, duration, VBR}},
 * lossless formats carry {@code {duration, sample rate, bit depth}}. An
 * attribute the header does not yield is omitted rather than sent as zero.
 *
 * <p>A probe never throws. A file that is not recognized audio, is truncated,
 * or lies about itself yields {@link FileAttributes#none()} — the same answer
 * a non-audio file gets, because a share full of documents is not an error.
 */
public final class AudioMetadata {

    /** How far into a file the MP3 frame sync is searched for. */
    private static final int MP3_SYNC_SEARCH_LIMIT = 64 * 1024;

    /** How much of the tail is searched for the last Ogg page. */
    private static final int OGG_TAIL_SEARCH = 64 * 1024;

    /** MPEG-1 sampling rates by header index; MPEG-2 is half, MPEG-2.5 a quarter. */
    private static final int[] MP3_SAMPLE_RATES = {44100, 48000, 32000};

    /** Bitrate tables in kbps, by header index 1..14. Index 0 is "free", 15 is invalid. */
    private static final int[] BITRATES_V1_L1 = {0, 32, 64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 384, 416, 448};

    private static final int[] BITRATES_V1_L2 = {0, 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384};
    private static final int[] BITRATES_V1_L3 = {0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320};
    private static final int[] BITRATES_V2_L1 = {0, 32, 48, 56, 64, 80, 96, 112, 128, 144, 160, 176, 192, 224, 256};
    private static final int[] BITRATES_V2_L2_L3 = {0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160};

    private AudioMetadata() {}

    /**
     * Probes a file for the attributes peers are shown.
     *
     * @param file the local file
     * @return its attributes, or none if it is not recognized audio
     */
    public static FileAttributes probe(Path file) {
        String name = String.valueOf(file.getFileName()).toLowerCase(Locale.ROOT);
        int dot = name.lastIndexOf('.');
        if (dot < 0) {
            return FileAttributes.none();
        }
        String extension = name.substring(dot + 1);
        // Decided before the file is opened, so a scan over a share full of
        // documents costs no opens at all.
        boolean recognized =
                switch (extension) {
                    case "mp3", "flac", "wav", "wave", "ogg", "oga", "opus", "m4a", "m4b", "mp4", "aac" -> true;
                    default -> false;
                };
        if (!recognized) {
            return FileAttributes.none();
        }
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            return switch (extension) {
                case "mp3" -> mp3(channel);
                case "flac" -> flac(channel);
                case "wav", "wave" -> wav(channel);
                case "ogg", "oga", "opus" -> ogg(channel);
                default -> mp4(channel);
            };
        } catch (IOException | RuntimeException unreadable) {
            // Unreadable, truncated, or a header that lies: the same silence a
            // non-audio file gets.
            return FileAttributes.none();
        }
    }

    // ---- publishing ---------------------------------------------------------

    /** Builds the lossy attribute set: bitrate, duration, VBR. */
    private static FileAttributes lossy(long bitrateKbps, long durationSeconds, boolean vbr) {
        Map<FileAttributeType, Integer> attributes = new LinkedHashMap<>();
        put(attributes, FileAttributeType.BIT_RATE, bitrateKbps);
        put(attributes, FileAttributeType.LENGTH, durationSeconds);
        if (!attributes.isEmpty()) {
            attributes.put(FileAttributeType.VARIABLE_BIT_RATE, vbr ? 1 : 0);
        }
        return attributes.isEmpty() ? FileAttributes.none() : new FileAttributes(attributes);
    }

    /** Builds the lossless attribute set: duration, sample rate, bit depth. */
    private static FileAttributes lossless(long durationSeconds, long sampleRate, long bitDepth) {
        Map<FileAttributeType, Integer> attributes = new LinkedHashMap<>();
        put(attributes, FileAttributeType.LENGTH, durationSeconds);
        put(attributes, FileAttributeType.SAMPLE_RATE, sampleRate);
        put(attributes, FileAttributeType.BIT_DEPTH, bitDepth);
        return attributes.isEmpty() ? FileAttributes.none() : new FileAttributes(attributes);
    }

    /** Records a value if it is positive and fits the wire's uint32. */
    private static void put(Map<FileAttributeType, Integer> attributes, FileAttributeType type, long value) {
        if (value > 0 && value <= Integer.MAX_VALUE) {
            attributes.put(type, (int) value);
        }
    }

    // ---- MP3 ---------------------------------------------------------------

    private static FileAttributes mp3(FileChannel channel) throws IOException {
        long offset = skipId3v2(channel);
        byte[] window = read(channel, offset, MP3_SYNC_SEARCH_LIMIT);

        for (int at = 0; at + 4 <= window.length; at++) {
            if ((window[at] & 0xFF) != 0xFF || (window[at + 1] & 0xE0) != 0xE0) {
                continue;
            }
            Mp3Frame frame = Mp3Frame.parse(window, at);
            if (frame == null) {
                continue;
            }
            // A sync pattern can occur in audio data; a real frame is followed
            // by another one. Only trust a header whose successor also parses,
            // unless the successor lies outside the window.
            int next = at + frame.frameLength;
            if (next + 4 <= window.length
                    && ((window[next] & 0xFF) != 0xFF
                            || (window[next + 1] & 0xE0) != 0xE0
                            || Mp3Frame.parse(window, next) == null)) {
                continue;
            }
            long audioBytes = channel.size() - offset - at;
            return frame.attributes(window, at, audioBytes);
        }
        return FileAttributes.none();
    }

    /** One parsed MP3 frame header, and the arithmetic that follows from it. */
    private record Mp3Frame(int samplesPerFrame, int sampleRate, int bitrateKbps, int frameLength, int xingOffset) {

        static Mp3Frame parse(byte[] data, int at) {
            int b1 = data[at + 1] & 0xFF;
            int b2 = data[at + 2] & 0xFF;
            int b3 = data[at + 3] & 0xFF;
            int version = (b1 >> 3) & 3; // 0=2.5, 2=2, 3=1
            int layer = (b1 >> 1) & 3; // 1=III, 2=II, 3=I
            int bitrateIndex = (b2 >> 4) & 0xF;
            int sampleRateIndex = (b2 >> 2) & 3;
            int padding = (b2 >> 1) & 1;
            int channelMode = (b3 >> 6) & 3;
            if (version == 1 || layer == 0 || bitrateIndex == 0 || bitrateIndex == 15 || sampleRateIndex == 3) {
                return null;
            }

            boolean v1 = version == 3;
            int[] bitrates = v1
                    ? (layer == 3 ? BITRATES_V1_L1 : layer == 2 ? BITRATES_V1_L2 : BITRATES_V1_L3)
                    : (layer == 3 ? BITRATES_V2_L1 : BITRATES_V2_L2_L3);
            int bitrate = bitrates[bitrateIndex];
            int sampleRate = MP3_SAMPLE_RATES[sampleRateIndex] / (v1 ? 1 : version == 2 ? 2 : 4);
            int samplesPerFrame = layer == 3 ? 384 : layer == 2 ? 1152 : v1 ? 1152 : 576;

            int frameLength = layer == 3
                    ? (12 * bitrate * 1000 / sampleRate + padding) * 4
                    : samplesPerFrame / 8 * bitrate * 1000 / sampleRate + padding;
            if (frameLength <= 4) {
                return null;
            }
            // The Xing/Info header sits after the side information, whose size
            // depends on version and channel mode. Only Layer III has one.
            int sideInformation = v1 ? (channelMode == 3 ? 17 : 32) : (channelMode == 3 ? 9 : 17);
            int xingOffset = layer == 1 ? 4 + sideInformation : -1;
            return new Mp3Frame(samplesPerFrame, sampleRate, bitrate, frameLength, xingOffset);
        }

        FileAttributes attributes(byte[] window, int at, long audioBytes) {
            long frames = -1;
            long streamBytes = -1;
            boolean vbr = false;

            if (xingOffset > 0 && at + xingOffset + 16 <= window.length) {
                int tag = at + xingOffset;
                if (matches(window, tag, "Xing") || matches(window, tag, "Info")) {
                    vbr = matches(window, tag, "Xing");
                    int flags = intAt(window, tag + 4);
                    int cursor = tag + 8;
                    if ((flags & 1) != 0) {
                        frames = intAt(window, cursor) & 0xFFFFFFFFL;
                        cursor += 4;
                    }
                    if ((flags & 2) != 0) {
                        streamBytes = intAt(window, cursor) & 0xFFFFFFFFL;
                    }
                } else if (at + 36 + 26 <= window.length && matches(window, at + 36, "VBRI")) {
                    vbr = true;
                    streamBytes = intAt(window, at + 36 + 10) & 0xFFFFFFFFL;
                    frames = intAt(window, at + 36 + 14) & 0xFFFFFFFFL;
                }
            }

            if (frames > 0) {
                long durationSeconds = Math.round((double) frames * samplesPerFrame / sampleRate);
                long bytes = streamBytes > 0 ? streamBytes : audioBytes;
                long kbps = vbr && durationSeconds > 0 ? Math.round(bytes * 8.0 / durationSeconds / 1000) : bitrateKbps;
                return lossy(kbps, durationSeconds, vbr);
            }
            // No frame count: a plain CBR stream, timed by its size.
            return lossy(bitrateKbps, Math.round(audioBytes * 8.0 / (bitrateKbps * 1000)), false);
        }
    }

    /** Returns the offset of the first byte after any ID3v2 tag. */
    private static long skipId3v2(FileChannel channel) throws IOException {
        byte[] header = read(channel, 0, 10);
        if (header.length < 10 || !matches(header, 0, "ID3")) {
            return 0;
        }
        long size = ((header[6] & 0x7F) << 21)
                | ((header[7] & 0x7F) << 14)
                | ((header[8] & 0x7F) << 7)
                | (header[9] & 0x7F);
        boolean footer = (header[5] & 0x10) != 0;
        return 10 + size + (footer ? 10 : 0);
    }

    // ---- FLAC --------------------------------------------------------------

    private static FileAttributes flac(FileChannel channel) throws IOException {
        long offset = skipId3v2(channel);
        byte[] magic = read(channel, offset, 4);
        if (!matches(magic, 0, "fLaC")) {
            return FileAttributes.none();
        }
        long at = offset + 4;
        for (int guard = 0; guard < 64; guard++) {
            byte[] head = read(channel, at, 4);
            if (head.length < 4) {
                return FileAttributes.none();
            }
            int type = head[0] & 0x7F;
            boolean last = (head[0] & 0x80) != 0;
            int length = ((head[1] & 0xFF) << 16) | ((head[2] & 0xFF) << 8) | (head[3] & 0xFF);
            if (type == 0 && length >= 18) {
                byte[] info = read(channel, at + 4, 18);
                if (info.length < 18) {
                    return FileAttributes.none();
                }
                int sampleRate = ((info[10] & 0xFF) << 12) | ((info[11] & 0xFF) << 4) | ((info[12] & 0xFF) >> 4);
                int bitDepth = (((info[12] & 1) << 4) | ((info[13] & 0xFF) >> 4)) + 1;
                long totalSamples = ((long) (info[13] & 0x0F) << 32)
                        | ((long) (info[14] & 0xFF) << 24)
                        | ((info[15] & 0xFF) << 16)
                        | ((info[16] & 0xFF) << 8)
                        | (info[17] & 0xFF);
                if (sampleRate <= 0) {
                    return FileAttributes.none();
                }
                return lossless(Math.round((double) totalSamples / sampleRate), sampleRate, bitDepth);
            }
            if (last) {
                return FileAttributes.none();
            }
            at += 4 + length;
        }
        return FileAttributes.none();
    }

    // ---- WAV ---------------------------------------------------------------

    private static FileAttributes wav(FileChannel channel) throws IOException {
        byte[] riff = read(channel, 0, 12);
        if (riff.length < 12 || !matches(riff, 0, "RIFF") || !matches(riff, 8, "WAVE")) {
            return FileAttributes.none();
        }
        long at = 12;
        long sampleRate = 0;
        long byteRate = 0;
        long bitDepth = 0;
        for (int guard = 0; guard < 64; guard++) {
            byte[] head = read(channel, at, 8);
            if (head.length < 8) {
                return FileAttributes.none();
            }
            long size = littleEndianIntAt(head, 4) & 0xFFFFFFFFL;
            if (matches(head, 0, "fmt ") && size >= 16) {
                byte[] fmt = read(channel, at + 8, 16);
                if (fmt.length < 16) {
                    return FileAttributes.none();
                }
                sampleRate = littleEndianIntAt(fmt, 4) & 0xFFFFFFFFL;
                byteRate = littleEndianIntAt(fmt, 8) & 0xFFFFFFFFL;
                bitDepth = ((fmt[15] & 0xFF) << 8) | (fmt[14] & 0xFF);
            } else if (matches(head, 0, "data")) {
                long durationSeconds = byteRate > 0 ? Math.round((double) size / byteRate) : 0;
                return lossless(durationSeconds, sampleRate, bitDepth);
            }
            at += 8 + size + (size & 1);
        }
        return FileAttributes.none();
    }

    // ---- Ogg (Vorbis and Opus) ---------------------------------------------

    private static FileAttributes ogg(FileChannel channel) throws IOException {
        byte[] head = read(channel, 0, 27 + 255 + 64);
        if (head.length < 28 || !matches(head, 0, "OggS")) {
            return FileAttributes.none();
        }
        int segments = head[26] & 0xFF;
        int payload = 27 + segments;
        if (payload + 30 > head.length) {
            return FileAttributes.none();
        }

        long granule = lastOggGranule(channel);

        if ((head[payload] & 0xFF) == 1 && matches(head, payload + 1, "vorbis")) {
            long sampleRate = littleEndianIntAt(head, payload + 12) & 0xFFFFFFFFL;
            long maximum = littleEndianIntAt(head, payload + 16);
            long nominal = littleEndianIntAt(head, payload + 20);
            long minimum = littleEndianIntAt(head, payload + 24);
            if (sampleRate <= 0) {
                return FileAttributes.none();
            }
            long durationSeconds = granule > 0 ? Math.round((double) granule / sampleRate) : 0;
            long kbps = nominal > 0
                    ? Math.round(nominal / 1000.0)
                    : durationSeconds > 0 ? Math.round(channel.size() * 8.0 / durationSeconds / 1000) : 0;
            boolean vbr = !(nominal > 0 && nominal == minimum && nominal == maximum);
            return lossy(kbps, durationSeconds, vbr);
        }
        if (matches(head, payload, "OpusHead")) {
            // Opus granules are always at 48 kHz, less the encoder's pre-skip.
            long preSkip = ((head[payload + 11] & 0xFF) << 8) | (head[payload + 10] & 0xFF);
            long durationSeconds = granule > preSkip ? Math.round((granule - preSkip) / 48000.0) : 0;
            long kbps = durationSeconds > 0 ? Math.round(channel.size() * 8.0 / durationSeconds / 1000) : 0;
            return lossy(kbps, durationSeconds, true);
        }
        return FileAttributes.none();
    }

    /** Returns the granule position of the file's last Ogg page, or -1. */
    private static long lastOggGranule(FileChannel channel) throws IOException {
        long start = Math.max(0, channel.size() - OGG_TAIL_SEARCH);
        byte[] tail = read(channel, start, (int) Math.min(channel.size() - start, OGG_TAIL_SEARCH));
        for (int at = tail.length - 27; at >= 0; at--) {
            if (matches(tail, at, "OggS")) {
                long granule = 0;
                for (int index = 7; index >= 0; index--) {
                    granule = (granule << 8) | (tail[at + 6 + index] & 0xFF);
                }
                return granule;
            }
        }
        return -1;
    }

    // ---- MP4 (AAC and ALAC) ------------------------------------------------

    private static FileAttributes mp4(FileChannel channel) throws IOException {
        byte[] moov = findBox(channel, 0, channel.size(), "moov", 0);
        if (moov == null) {
            return FileAttributes.none();
        }

        long durationSeconds = 0;
        byte[] mvhd = findBoxIn(moov, "mvhd");
        if (mvhd != null && mvhd.length >= 4) {
            int version = mvhd[0] & 0xFF;
            if (version == 0 && mvhd.length >= 20) {
                long timescale = intAt(mvhd, 12) & 0xFFFFFFFFL;
                long duration = intAt(mvhd, 16) & 0xFFFFFFFFL;
                durationSeconds = timescale > 0 ? Math.round((double) duration / timescale) : 0;
            } else if (version == 1 && mvhd.length >= 32) {
                long timescale = intAt(mvhd, 20) & 0xFFFFFFFFL;
                long duration = longAt(mvhd, 24);
                durationSeconds = timescale > 0 ? Math.round((double) duration / timescale) : 0;
            }
        }

        byte[] stsd = descend(moov, "trak", "mdia", "minf", "stbl", "stsd");
        if (stsd == null || stsd.length < 16) {
            return FileAttributes.none();
        }
        // stsd: version+flags, entry count, then sample entries.
        int entrySize = intAt(stsd, 8);
        if (entrySize < 36 || 8 + entrySize > stsd.length) {
            return FileAttributes.none();
        }
        String format = new String(stsd, 12, 4, java.nio.charset.StandardCharsets.US_ASCII);
        // The audio sample entry: 8 bytes of size+type, 6 reserved, 2 data
        // reference index, 8 of version/revision/vendor, then channel count,
        // sample size at 26, and the 16.16 sample rate at 32.
        int entry = 8;
        long sampleRate = (intAt(stsd, entry + 32) >>> 16) & 0xFFFF;
        long sampleSize = ((stsd[entry + 26] & 0xFF) << 8) | (stsd[entry + 27] & 0xFF);
        byte[] children = new byte[Math.max(0, entrySize - 36)];
        System.arraycopy(stsd, entry + 36, children, 0, children.length);

        if ("alac".equals(format)) {
            byte[] cookie = findBoxIn(children, "alac");
            if (cookie != null && cookie.length >= 10) {
                // The magic cookie: version+flags, frame length, compatible
                // version, then the bit depth.
                sampleSize = cookie[9] & 0xFF;
                if (cookie.length >= 28) {
                    sampleRate = intAt(cookie, 24) & 0xFFFFFFFFL;
                }
            }
            return lossless(durationSeconds, sampleRate, sampleSize);
        }
        if ("mp4a".equals(format)) {
            long kbps = esdsAverageBitrate(children);
            if (kbps <= 0 && durationSeconds > 0) {
                kbps = Math.round(channel.size() * 8.0 / durationSeconds / 1000);
            }
            return lossy(kbps, durationSeconds, false);
        }
        return FileAttributes.none();
    }

    /** Reads the esds descriptor's average bitrate in kbps, or -1. */
    private static long esdsAverageBitrate(byte[] children) {
        byte[] esds = findBoxIn(children, "esds");
        if (esds == null) {
            return -1;
        }
        // version+flags, then an ES descriptor (tag 0x03), inside which sits
        // the decoder configuration (tag 0x04) carrying the bitrates.
        int at = 4;
        if (at >= esds.length || (esds[at] & 0xFF) != 0x03) {
            return -1;
        }
        at = skipDescriptorLength(esds, at + 1);
        at += 3; // ES id and flags
        if (at >= esds.length || (esds[at] & 0xFF) != 0x04) {
            return -1;
        }
        at = skipDescriptorLength(esds, at + 1);
        at += 1 + 4 + 4; // object type, stream type + buffer size, maximum bitrate
        if (at + 4 > esds.length) {
            return -1;
        }
        long average = intAt(esds, at) & 0xFFFFFFFFL;
        return average > 0 ? Math.round(average / 1000.0) : -1;
    }

    /** Steps over a descriptor's variable-length size field. */
    private static int skipDescriptorLength(byte[] data, int at) {
        while (at < data.length && (data[at] & 0x80) != 0) {
            at++;
        }
        return at + 1;
    }

    /** Walks nested boxes from a parent's payload, returning the last payload. */
    private static byte[] descend(byte[] payload, String... path) {
        byte[] current = payload;
        for (String name : path) {
            current = findBoxIn(current, name);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    /** Finds a box by name inside an in-memory payload, returning its payload. */
    private static byte[] findBoxIn(byte[] payload, String name) {
        int at = 0;
        for (int guard = 0; guard < 256 && at + 8 <= payload.length; guard++) {
            long size = intAt(payload, at) & 0xFFFFFFFFL;
            if (size < 8 || at + size > payload.length) {
                return null;
            }
            if (matches(payload, at + 4, name)) {
                byte[] inner = new byte[(int) size - 8];
                System.arraycopy(payload, at + 8, inner, 0, inner.length);
                return inner;
            }
            at += size;
        }
        return null;
    }

    /** Finds a top-level box by seeking, returning its payload read whole. */
    private static byte[] findBox(FileChannel channel, long from, long limit, String name, int depth)
            throws IOException {
        long at = from;
        for (int guard = 0; guard < 256 && at + 8 <= limit; guard++) {
            byte[] head = read(channel, at, 16);
            if (head.length < 8) {
                return null;
            }
            long size = intAt(head, 0) & 0xFFFFFFFFL;
            long headerLength = 8;
            if (size == 1 && head.length >= 16) {
                size = longAt(head, 8);
                headerLength = 16;
            } else if (size == 0) {
                size = limit - at;
            }
            if (size < headerLength || at + size > limit) {
                return null;
            }
            if (matches(head, 4, name)) {
                long payload = size - headerLength;
                if (payload > 32 * 1024 * 1024) {
                    // A moov this size is not one; refuse to buffer it.
                    return null;
                }
                return read(channel, at + headerLength, (int) payload);
            }
            at += size;
        }
        return null;
    }

    // ---- reading -----------------------------------------------------------

    /** Reads up to {@code length} bytes at a position; short at end of file. */
    private static byte[] read(FileChannel channel, long position, int length) throws IOException {
        long available = channel.size() - position;
        if (available <= 0) {
            return new byte[0];
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(length, available));
        int at = 0;
        while (buffer.hasRemaining()) {
            int count = channel.read(buffer, position + at);
            if (count < 0) {
                break;
            }
            at += count;
        }
        byte[] bytes = new byte[buffer.position()];
        buffer.flip();
        buffer.get(bytes);
        return bytes;
    }

    private static boolean matches(byte[] data, int at, String text) {
        if (at < 0 || at + text.length() > data.length) {
            return false;
        }
        for (int index = 0; index < text.length(); index++) {
            if ((data[at + index] & 0xFF) != text.charAt(index)) {
                return false;
            }
        }
        return true;
    }

    private static int intAt(byte[] data, int at) {
        return ((data[at] & 0xFF) << 24)
                | ((data[at + 1] & 0xFF) << 16)
                | ((data[at + 2] & 0xFF) << 8)
                | (data[at + 3] & 0xFF);
    }

    private static long longAt(byte[] data, int at) {
        return ((long) intAt(data, at) << 32) | (intAt(data, at + 4) & 0xFFFFFFFFL);
    }

    private static int littleEndianIntAt(byte[] data, int at) {
        return ((data[at + 3] & 0xFF) << 24)
                | ((data[at + 2] & 0xFF) << 16)
                | ((data[at + 1] & 0xFF) << 8)
                | (data[at] & 0xFF);
    }
}
