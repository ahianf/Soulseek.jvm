// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.concurrent;

import java.util.concurrent.CancellationException;

/** Preserves an internal wait's interrupt through the unchecked engine layers. */
public final class InterruptedOperationException extends CancellationException {

    public InterruptedOperationException(String message, InterruptedException cause) {
        super(message);
        initCause(cause);
    }
}
