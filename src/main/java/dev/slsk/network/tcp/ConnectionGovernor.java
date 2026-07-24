// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import dev.slsk.CancellationToken;
import java.util.concurrent.CompletableFuture;

/** Asynchronously grants bytes for a connection read or write. */
@FunctionalInterface
public interface ConnectionGovernor {
    /** Grants some or all of the requested bytes. */
    CompletableFuture<Integer> grantAsync(int requestedBytes, CancellationToken cancellationToken);
}
