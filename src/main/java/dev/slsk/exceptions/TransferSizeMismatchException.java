// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/**
 * Indicates that a remote file size does not match the locally specified size.
 */
public class TransferSizeMismatchException extends SoulseekClientException {
    private static final long serialVersionUID = 1L;

    private final long localSize;
    private final long remoteSize;

    public TransferSizeMismatchException(long localSize, long remoteSize) {
        this.localSize = localSize;
        this.remoteSize = remoteSize;
    }

    public TransferSizeMismatchException(String message, long localSize, long remoteSize) {
        super(message);
        this.localSize = localSize;
        this.remoteSize = remoteSize;
    }

    public TransferSizeMismatchException(String message, long localSize, long remoteSize, Throwable cause) {
        super(message, cause);
        this.localSize = localSize;
        this.remoteSize = remoteSize;
    }

    public long getRemoteSize() {
        return remoteSize;
    }

    public long getLocalSize() {
        return localSize;
    }
}
