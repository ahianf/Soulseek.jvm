// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

/** The mutually exclusive origin and establishment mode of a connection. */
public enum ConnectionType {
    /** The connection has not yet been classified. */
    UNCLASSIFIED,
    /** A directly accepted inbound connection. */
    INBOUND_DIRECT,
    /** An indirectly solicited inbound connection. */
    INBOUND_INDIRECT,
    /** A directly established outbound connection. */
    OUTBOUND_DIRECT,
    /** An indirectly established outbound connection. */
    OUTBOUND_INDIRECT
}
