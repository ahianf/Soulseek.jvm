// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import java.io.InputStream;
import java.util.Objects;

/** Raw search-response bytes retained for protocol tests and adapters. */
public record RawSearchResponse(long length, InputStream stream) {
    public RawSearchResponse {
        if (length <= 0) {
            throw new IllegalArgumentException("The response length must be greater than zero");
        }
        stream = Objects.requireNonNull(stream, "The specified input stream is null");
    }
}
