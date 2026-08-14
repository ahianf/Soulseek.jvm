// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Information about the distributed network.
 */
public class DistributedNetworkInfo {
    private final Double averageBroadcastLatency;
    private final int branchLevel;
    private final String branchRoot;
    private final boolean acceptChildren;
    private final int childLimit;
    private final List<DistributedPeer> children;
    private final boolean parentPresent;
    private final boolean branchRootNode;
    private final DistributedPeer parent;

    /**
     * Creates distributed-network information.
     *
     * @param averageBroadcastLatency the average child broadcast latency
     * @param branchLevel the current branch level
     * @param branchRoot the current branch root
     * @param isBranchRoot whether the client is operating as a branch root
     * @param childLimit the allowed concurrent child count
     * @param canAcceptChildren whether child connections can be accepted
     * @param children the current child connections
     * @param parent the current parent tuple
     * @param hasParent whether a parent connection is established
     */
    public DistributedNetworkInfo(
            Double averageBroadcastLatency,
            int branchLevel,
            String branchRoot,
            boolean isBranchRoot,
            int childLimit,
            boolean canAcceptChildren,
            Iterable<? extends DistributedPeer> children,
            DistributedPeer parent,
            boolean hasParent) {
        this.averageBroadcastLatency = averageBroadcastLatency;
        this.branchLevel = branchLevel;
        this.branchRoot = branchRoot;
        this.acceptChildren = canAcceptChildren;
        this.childLimit = childLimit;
        this.children = children == null ? null : immutableCopy(children);
        this.parentPresent = hasParent;
        this.branchRootNode = isBranchRoot;
        this.parent = Objects.requireNonNull(parent, "parent");
    }

    /**
     * Returns the average child broadcast latency.
     *
     * @return the latency, or {@code null}
     */
    public final Double getAverageBroadcastLatency() {
        return averageBroadcastLatency;
    }

    /**
     * Returns the current branch level.
     *
     * @return the branch level
     */
    public final int getBranchLevel() {
        return branchLevel;
    }

    /**
     * Returns the current branch root.
     *
     * @return the branch root
     */
    public final String getBranchRoot() {
        return branchRoot;
    }

    /**
     * Returns whether child connections can be accepted.
     *
     * @return whether children can be accepted
     */
    public final boolean canAcceptChildren() {
        return acceptChildren;
    }

    /**
     * Returns the allowed concurrent child count.
     *
     * @return the child limit
     */
    public final int getChildLimit() {
        return childLimit;
    }

    /**
     * Returns the current child connections.
     *
     * @return an immutable snapshot, or {@code null}
     */
    public final List<DistributedPeer> getChildren() {
        return children;
    }

    /**
     * Returns whether a parent connection is established.
     *
     * @return whether a parent is established
     */
    public final boolean hasParent() {
        return parentPresent;
    }

    /**
     * Returns whether the client is operating as a branch root.
     *
     * @return whether the client is a branch root
     */
    public final boolean isBranchRoot() {
        return branchRootNode;
    }

    /**
     * Returns the current parent tuple.
     *
     * @return the parent tuple
     */
    public final DistributedPeer getParent() {
        return parent;
    }

    private static List<DistributedPeer> immutableCopy(Iterable<? extends DistributedPeer> source) {
        List<DistributedPeer> copy = new ArrayList<>();
        source.forEach(peer -> copy.add(Objects.requireNonNull(peer, "children element")));
        return Collections.unmodifiableList(copy);
    }
}
