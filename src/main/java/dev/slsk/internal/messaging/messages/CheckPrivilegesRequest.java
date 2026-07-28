// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;

/** Requests the local user's privilege status. */
public final class CheckPrivilegesRequest extends EmptyServerMessage {
    public CheckPrivilegesRequest() {
        super(MessageCode.Server.CHECK_PRIVILEGES);
    }
}
