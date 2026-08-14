// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.transfer;

/**
 * Where a transfer sits in the library's own queue.
 *
 * <p>This orders work <em>we</em> have not started yet. It says nothing to a
 * peer and nothing about the position a peer gives us once we have asked: the
 * remote queue is the remote's business, and no client can jump it by asking
 * differently. Raising a queued download's priority moves it ahead of our other
 * queued downloads, and that is all.
 *
 * <p>Three levels, because the useful distinctions are "do this next", "do this
 * normally", and "do this when nothing else wants the slot". Finer grades invite
 * a consumer to encode a total ordering in a field that is really a hint.
 */
public enum Priority {

    /** Runs only when no {@link #NORMAL} or {@link #HIGH} work wants the slot. */
    LOW,

    /** The default. */
    NORMAL,

    /** Runs ahead of {@link #NORMAL} and {@link #LOW} work. */
    HIGH
}
