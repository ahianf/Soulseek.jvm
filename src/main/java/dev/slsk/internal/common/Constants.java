// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

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
}
