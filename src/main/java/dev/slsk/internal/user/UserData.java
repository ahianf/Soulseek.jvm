// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.user;

import java.util.Objects;

/** User data returned by the server. */
public record UserData(
        String username,
        UserPresence status,
        int averageSpeed,
        long uploadCount,
        int fileCount,
        int directoryCount,
        String countryCode,
        Integer slotsFree) {
    public UserData {
        status = Objects.requireNonNull(status, "status");
    }

    /** Creates user data without a free-slot count. */
    public UserData(
            String username,
            UserPresence status,
            int averageSpeed,
            long uploadCount,
            int fileCount,
            int directoryCount,
            String countryCode) {
        this(username, status, averageSpeed, uploadCount, fileCount, directoryCount, countryCode, null);
    }
}
