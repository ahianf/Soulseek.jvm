// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * Event arguments raised when the distributed parent connection changes.
 */
public class DistributedParentEvent extends SoulseekClientEvent {
    private final int branchLevel;
    private final String branchRoot;
    private final InetSocketAddress ipEndpoint;
    private final boolean branchRootNode;
    private final String username;

    /**
     * Creates distributed-parent event payload.
     *
     * @param username the username associated with the connection
     * @param ipEndpoint the connection endpoint
     * @param branchLevel the parent's branch level
     * @param branchRoot the root of the distributed branch
     */
    public DistributedParentEvent(String username, InetSocketAddress ipEndpoint, int branchLevel, String branchRoot) {
        this.username = username;
        this.ipEndpoint = ipEndpoint;
        this.branchLevel = branchLevel;
        this.branchRoot = branchRoot;
        this.branchRootNode = Objects.equals(username, branchRoot) && branchLevel == 0;
    }

    /**
     * Returns the parent's branch level.
     *
     * @return the branch level
     */
    public final int getBranchLevel() {
        return branchLevel;
    }

    /**
     * Returns the root of the distributed branch.
     *
     * @return the branch root
     */
    public final String getBranchRoot() {
        return branchRoot;
    }

    /**
     * Returns the connection endpoint.
     *
     * @return the IP endpoint
     */
    public final InetSocketAddress getIpEndpoint() {
        return ipEndpoint;
    }

    /**
     * Returns whether the parent is the branch root.
     *
     * @return {@code true} when the parent is the branch root
     */
    public final boolean isBranchRoot() {
        return branchRootNode;
    }

    /**
     * Returns the username associated with the connection.
     *
     * @return the username
     */
    public final String getUsername() {
        return username;
    }
}
