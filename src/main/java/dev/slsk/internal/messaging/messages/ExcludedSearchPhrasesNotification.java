// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import java.util.List;

/** Parses the server's list of excluded search phrases. */
public final class ExcludedSearchPhrasesNotification implements IncomingMessage {

    private ExcludedSearchPhrasesNotification() {}

    /** Parses the immutable excluded-phrase list. */
    public static List<String> fromByteArray(byte[] bytes) {
        return ServerStringListNotification.parse(
                bytes, MessageCode.Server.EXCLUDED_SEARCH_PHRASES, "ExcludedSearchPhrasesNotification");
    }
}
