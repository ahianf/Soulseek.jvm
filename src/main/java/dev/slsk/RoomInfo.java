// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.Objects;

/**
 * A room in the server's directory, before we have joined it.
 *
 * @param name the room name
 * @param userCount how many people are in it
 */
public record RoomInfo(String name, int userCount) {

    /** Validates and returns the info. */
    public RoomInfo {
        Objects.requireNonNull(name, "name");
    }
}
