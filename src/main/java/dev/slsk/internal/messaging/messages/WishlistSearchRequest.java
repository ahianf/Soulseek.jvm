// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;

/** Requests a wishlist search. */
public final class WishlistSearchRequest implements OutgoingMessage {
    private final String searchText;
    private final int token;

    public WishlistSearchRequest(String searchText, int token) {
        this.searchText = searchText;
        this.token = token;
    }

    public String getSearchText() {
        return searchText;
    }

    public int getToken() {
        return token;
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.WISHLIST_SEARCH)
                .writeInteger(token)
                .writeString(searchText)
                .build();
    }
}
