// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.common;

/**
 * Assembly-internal application constants.
 *
 * <p>This class is public only so Java implementation subpackages can share
 * the C# assembly-internal values. Its package is not exported by the module.
 */
public final class Constants {
    /** Soulseek network major version. */
    public static final int MAJOR_VERSION = 170;

    private Constants() {}

    /** SocketConnection-establishment method names. */
    public static final class ConnectionMethod {
        /** Direct establishment. */
        public static final String DIRECT = "Direct";
        /** Indirect establishment. */
        public static final String INDIRECT = "Indirect";

        private ConnectionMethod() {}
    }

    /** Soulseek peer-connection type identifiers. */
    public static final class ConnectionType {
        /** Distributed connection. */
        public static final String DISTRIBUTED = "D";
        /** Peer message connection. */
        public static final String PEER = "P";
        /** File transfer connection. */
        public static final String TRANSFER = "F";

        private ConnectionType() {}
    }

    /** Correlation wait-key prefixes. */
    public static final class WaitKey {
        /** Branch-level message. */
        public static final String BRANCH_LEVEL_MESSAGE = "BranchLevelMessage";
        /** Branch-root message. */
        public static final String BRANCH_ROOT_MESSAGE = "BranchRootMessage";
        /** Browse-response connection. */
        public static final String BROWSE_RESPONSE_CONNECTION = "BrowseResponseConnection";
        /** Child-depth message. */
        public static final String CHILD_DEPTH_MESSAGE = "ChildDepthMessage";
        /** Direct transfer. */
        public static final String DIRECT_TRANSFER = "DirectTransfer";
        /** Indirect transfer. */
        public static final String INDIRECT_TRANSFER = "IndirectTransfer";
        /** Search-request message. */
        public static final String SEARCH_REQUEST_MESSAGE = "SearchRequestMessage";
        /** Solicited distributed connection. */
        public static final String SOLICITED_DISTRIBUTED_CONNECTION = "SolicitedDistributedConnection";
        /** Solicited peer connection. */
        public static final String SOLICITED_PEER_CONNECTION = "SolicitedPeerConnection";
        /** Transfer state wait. */
        public static final String TRANSFER = "Transfer";

        private WaitKey() {}
    }
}
