// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import java.util.Objects;

/** A file attribute. */
public record FileAttribute(FileAttributeType type, int value) {
    public FileAttribute {
        type = Objects.requireNonNull(type, "type");
    }
}
