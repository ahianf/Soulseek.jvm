// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A file directory within a peer's shared files.
 */
public class Directory {
    private final int fileCount;
    private final List<File> files;
    private final String name;

    /**
     * Creates an empty directory.
     *
     * @param name the directory name
     */
    public Directory(String name) {
        this(name, null);
    }

    /**
     * Creates a directory.
     *
     * @param name the directory name
     * @param fileList the optional sequence of files
     */
    public Directory(String name, Iterable<? extends File> fileList) {
        this.name = name;

        List<File> copiedFiles = new ArrayList<>();
        if (fileList != null) {
            fileList.forEach(copiedFiles::add);
        }
        files = Collections.unmodifiableList(copiedFiles);
        fileCount = files.size();
    }

    /**
     * Returns the directory name.
     *
     * @return the directory name
     */
    public final String getName() {
        return name;
    }

    /**
     * Returns the number of files in the directory.
     *
     * @return the file count
     */
    public final int getFileCount() {
        return fileCount;
    }

    /**
     * Returns the files as an immutable snapshot.
     *
     * @return the files
     */
    public final List<File> getFiles() {
        return files;
    }
}
