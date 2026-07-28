// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;

/** Requests a search from one user. */
public final class UserSearchRequest implements OutgoingMessage {
    private final String searchText;
    private final int token;
    private final String username;

    public UserSearchRequest(String username, String searchText, int token) {
        this.username = username;
        this.searchText = searchText;
        this.token = token;
    }

    public String getSearchText() {
        return searchText;
    }

    public int getToken() {
        return token;
    }

    public String getUsername() {
        return username;
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.USER_SEARCH)
                .writeString(username)
                .writeInteger(token)
                .writeString(searchText)
                .build();
    }
}
