// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.diagnostics;

import dev.slsk.Username;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Our position in the distributed search mesh.
 *
 * <p>Soulseek distributes search traffic through a tree of peers rather than
 * through the server. A client adopts a parent, accepts children, and forwards
 * searches down; a client with no parent that has been promoted becomes a branch
 * root. None of it is something a consumer drives, and this is deliberately not
 * a control surface — there is no way here to choose a parent or refuse a child,
 * because those are decisions the protocol makes on its own terms.
 *
 * <p>It is, however, something a consumer renders. That is why it exists at all:
 * one snapshot in place of the seven separate listeners it took to assemble the
 * same picture by hand, each firing a fragment and leaving the consumer to hold
 * the rest.
 *
 * @param hasParent whether we have adopted a parent
 * @param parent the parent, if any
 * @param children the children we have accepted
 * @param isBranchRoot whether we are the root of our branch
 * @param branchLevel how deep we sit, counting from the root
 * @param branchRoot the root of our branch, if known
 */
public record MeshState(
        boolean hasParent,
        Optional<Username> parent,
        List<Username> children,
        boolean isBranchRoot,
        int branchLevel,
        Optional<Username> branchRoot) {

    /** Validates and returns the state. */
    public MeshState {
        Objects.requireNonNull(parent, "parent");
        Objects.requireNonNull(branchRoot, "branchRoot");
        children = List.copyOf(Objects.requireNonNull(children, "children"));
    }

    /**
     * Returns how many children we have accepted.
     *
     * @return the child count
     */
    public int childCount() {
        return children.size();
    }

    /**
     * Returns whether we are connected to the mesh at all.
     *
     * @return {@code true} if we have a parent or are a branch root
     */
    public boolean isConnected() {
        return hasParent || isBranchRoot;
    }
}
