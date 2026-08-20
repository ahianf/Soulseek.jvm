// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.network.tcp.ConnectionKey;
import dev.slsk.internal.transfer.TransferDirection;
import java.util.Objects;
import java.util.UUID;

/** A typed identity for one kind of correlated wait. */
public sealed interface WaitKey {

    /** A server response identified by message code alone. */
    record ServerMessage(MessageCode.Server code) implements WaitKey {
        public ServerMessage {
            Objects.requireNonNull(code, "code");
        }
    }

    /** A server response correlated to a user. */
    record ServerUser(MessageCode.Server code, String username) implements WaitKey {
        public ServerUser {
            Objects.requireNonNull(code, "code");
            Objects.requireNonNull(username, "username");
        }
    }

    /** A server response correlated to a room. */
    record ServerRoom(MessageCode.Server code, String room) implements WaitKey {
        public ServerRoom {
            Objects.requireNonNull(code, "code");
            Objects.requireNonNull(room, "room");
        }
    }

    /** A server response correlated to a room member or operator. */
    record ServerRoomUser(MessageCode.Server code, String room, String username) implements WaitKey {
        public ServerRoomUser {
            Objects.requireNonNull(code, "code");
            Objects.requireNonNull(room, "room");
            Objects.requireNonNull(username, "username");
        }
    }

    /** A peer response correlated to a user. */
    record PeerUser(MessageCode.Peer code, String username) implements WaitKey {
        public PeerUser {
            Objects.requireNonNull(code, "code");
            Objects.requireNonNull(username, "username");
        }
    }

    /** A peer response correlated to a user and protocol token. */
    record PeerToken(MessageCode.Peer code, String username, int token) implements WaitKey {
        public PeerToken {
            Objects.requireNonNull(code, "code");
            Objects.requireNonNull(username, "username");
        }
    }

    /** A peer response correlated to a user and remote path. */
    record PeerFile(MessageCode.Peer code, String username, String filename) implements WaitKey {
        public PeerFile {
            Objects.requireNonNull(code, "code");
            Objects.requireNonNull(username, "username");
            Objects.requireNonNull(filename, "filename");
        }
    }

    /** A distributed response correlated to a user. */
    record DistributedUser(MessageCode.Distributed code, String username) implements WaitKey {
        public DistributedUser {
            Objects.requireNonNull(code, "code");
            Objects.requireNonNull(username, "username");
        }
    }

    /** One transfer's terminal-state wait. */
    record Transfer(TransferDirection direction, String username, String filename, int token) implements WaitKey {
        public Transfer {
            Objects.requireNonNull(direction, "direction");
            Objects.requireNonNull(username, "username");
            Objects.requireNonNull(filename, "filename");
        }
    }

    /** An inbound direct transfer connection. */
    record DirectTransfer(String username, int token) implements WaitKey {
        public DirectTransfer {
            Objects.requireNonNull(username, "username");
        }
    }

    /** An inbound indirect transfer connection. */
    record IndirectTransfer(String username, String filename, int token) implements WaitKey {
        public IndirectTransfer {
            Objects.requireNonNull(username, "username");
            Objects.requireNonNull(filename, "filename");
        }
    }

    /** A requested inbound peer connection. */
    record SolicitedPeer(String username, int token) implements WaitKey {
        public SolicitedPeer {
            Objects.requireNonNull(username, "username");
        }
    }

    /** A requested inbound distributed connection. */
    record SolicitedDistributed(String username, int token) implements WaitKey {
        public SolicitedDistributed {
            Objects.requireNonNull(username, "username");
        }
    }

    /** The connection on which a browse response will arrive. */
    record BrowseResponseConnection(String username) implements WaitKey {
        public BrowseResponseConnection {
            Objects.requireNonNull(username, "username");
        }
    }

    /** A child-depth reply on one distributed connection. */
    record ChildDepth(ConnectionKey connection) implements WaitKey {
        public ChildDepth {
            Objects.requireNonNull(connection, "connection");
        }
    }

    /** A branch-level reply on one distributed connection. */
    record BranchLevel(UUID connectionId) implements WaitKey {
        public BranchLevel {
            Objects.requireNonNull(connectionId, "connectionId");
        }
    }

    /** A branch-root reply on one distributed connection. */
    record BranchRoot(UUID connectionId) implements WaitKey {
        public BranchRoot {
            Objects.requireNonNull(connectionId, "connectionId");
        }
    }

    /** The first search request observed on one distributed connection. */
    record SearchRequest(UUID connectionId) implements WaitKey {
        public SearchRequest {
            Objects.requireNonNull(connectionId, "connectionId");
        }
    }

    /** A local key for infrastructure that does not correlate a protocol message. */
    record Named(String name) implements WaitKey {
        public Named {
            Objects.requireNonNull(name, "name");
        }
    }
}
