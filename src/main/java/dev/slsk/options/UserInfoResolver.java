// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import dev.slsk.UserInfo;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/** Resolves a response to an incoming user-information request. */
@FunctionalInterface
public interface UserInfoResolver {
    /**
     * Resolves user information.
     *
     * @param username the requesting username
     * @param endpoint the requesting endpoint
     * @return the asynchronous response
     */
    CompletableFuture<UserInfo> resolve(String username, InetSocketAddress endpoint);
}
