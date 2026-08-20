// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import java.net.InetSocketAddress;
import java.util.Objects;

/** Event payload emitted when the distributed parent connection changes. */
public record DistributedParentEvent(String username, InetSocketAddress ipEndpoint, int branchLevel, String branchRoot)
        implements SoulseekClientEvent {

    /** Returns whether the parent is the branch root. */
    public boolean branchRootNode() {
        return Objects.equals(username, branchRoot) && branchLevel == 0;
    }
}
