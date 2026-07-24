// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.common;

/**
 * Holds the source-compatible process-wide event dispatch preference.
 *
 * <p>The public client surface owns this setting once that layer is
 * constructed; transport code uses it without depending on the client.</p>
 */
public final class EventDispatch {
    private static volatile boolean asynchronous;

    private EventDispatch() {}

    /** Returns whether progress events are dispatched asynchronously. */
    public static boolean isAsynchronous() {
        return asynchronous;
    }

    /** Sets whether progress events are dispatched asynchronously. */
    public static void setAsynchronous(boolean value) {
        asynchronous = value;
    }
}
