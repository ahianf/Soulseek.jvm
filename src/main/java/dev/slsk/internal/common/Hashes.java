// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Internal cryptographic and protocol hash encodings. */
public final class Hashes {
    private Hashes() {}

    /** Returns the lowercase hexadecimal MD5 digest of a UTF-8 string. */
    public static String md5Hex(String value) {
        Objects.requireNonNull(value, "value");
        try {
            byte[] digest = MessageDigest.getInstance("MD5").digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("The Java runtime does not provide MD5", exception);
        }
    }
}
