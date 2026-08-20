// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.user;

import java.util.Objects;

/** A user's presence and privilege state. */
public record UserStatusSnapshot(String username, WireUserPresence presence, boolean privileged) {
    public UserStatusSnapshot {
        presence = Objects.requireNonNull(presence, "presence");
    }
}
