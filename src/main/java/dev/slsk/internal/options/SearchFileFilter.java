// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import dev.slsk.internal.share.File;

/** Determines whether a file is accepted from a search response. */
@FunctionalInterface
public interface SearchFileFilter {
    /**
     * Tests a file.
     *
     * @param file the file
     * @return whether the file is accepted
     */
    boolean test(File file);
}
