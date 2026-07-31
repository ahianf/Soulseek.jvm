// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-FileCopyrightText: aioslsk contributors
// SPDX-FileCopyrightText: Nicotine+ Contributors
// SPDX-License-Identifier: GPL-3.0-only

// GENERATED — edit tools/wire-vectors/ and re-run generate.py. Do not hand-edit.

package dev.slsk.internal.messaging.vectors;

/** Shared hex decoding for the generated wire-vector suites. */
final class WireVectors {
    private WireVectors() {}

    /** Decodes a lower-case hex string into the framed message bytes it denotes. */
    static byte[] hex(String value) {
        int length = value.length();
        if (length % 2 != 0) {
            throw new IllegalArgumentException("Hex vector must have an even length: " + length);
        }
        byte[] out = new byte[length / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) ((digit(value.charAt(i * 2)) << 4) | digit(value.charAt(i * 2 + 1)));
        }
        return out;
    }

    private static int digit(char c) {
        int value = Character.digit(c, 16);
        if (value < 0) {
            throw new IllegalArgumentException("Not a hex digit: " + c);
        }
        return value;
    }
}
