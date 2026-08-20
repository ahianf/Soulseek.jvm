// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import java.util.List;
import java.util.Objects;

/** Information about the distributed network. */
public record DistributedNetworkInfo(
        Double averageBroadcastLatency,
        int branchLevel,
        String branchRoot,
        boolean branchRootNode,
        int childLimit,
        boolean acceptChildren,
        List<DistributedPeer> children,
        DistributedPeer parent,
        boolean parentPresent) {

    public DistributedNetworkInfo {
        children = children == null ? null : List.copyOf(children);
        parent = Objects.requireNonNull(parent, "parent");
    }
}
