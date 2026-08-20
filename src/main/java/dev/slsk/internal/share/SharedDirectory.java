// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import java.util.List;

/** A file directory within a peer's shared files. */
public record SharedDirectory(String name, List<File> files) {
    public SharedDirectory {
        files = files == null ? List.of() : List.copyOf(files);
    }

    /** Creates an empty directory. */
    public SharedDirectory(String name) {
        this(name, List.of());
    }

    /** Returns the number of files in the directory. */
    public int fileCount() {
        return files.size();
    }
}
