// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

/** Handles an event raised by {@link SoulseekClient}. */
@FunctionalInterface
public interface SoulseekClientEventListener<T> {
    void handle(Object sender, T eventArgs);
}
