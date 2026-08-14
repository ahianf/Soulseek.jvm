// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.room.Room;
import dev.slsk.room.RoomList;
import dev.slsk.room.RoomTicker;
import dev.slsk.room.RoomUser;
import dev.slsk.user.Username;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * What happened in the rooms we are in.
 *
 * <p>{@link MessageReceived} is the one event here that is not a delta on room
 * state, and it is the reason {@link Room} carries no message list: the message
 * is delivered, and what the consumer does with it — render it, store it, drop
 * it — is the consumer's decision. The library keeps the membership and the
 * tickers, which the server replaces wholesale, and keeps no scrollback.
 */
public sealed interface RoomEvent extends SoulseekEvent {

    /** We joined a room. */
    record Joined(String room, Room state, Instant at) implements RoomEvent {
        public Joined {
            Objects.requireNonNull(room, "room");
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(at, "at");
        }
    }

    /** We left a room. */
    record Left(String room, Instant at) implements RoomEvent {
        public Left {
            Objects.requireNonNull(room, "room");
            Objects.requireNonNull(at, "at");
        }
    }

    /** Somebody said something in a room we are in. */
    record MessageReceived(String room, Username from, String message, Instant at) implements RoomEvent {
        public MessageReceived {
            Objects.requireNonNull(room, "room");
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(message, "message");
            Objects.requireNonNull(at, "at");
        }
    }

    /** Somebody joined a room we are in. */
    record UserJoined(String room, RoomUser user, Instant at) implements RoomEvent {
        public UserJoined {
            Objects.requireNonNull(room, "room");
            Objects.requireNonNull(user, "user");
            Objects.requireNonNull(at, "at");
        }
    }

    /** Somebody left a room we are in. */
    record UserLeft(String room, Username user, Instant at) implements RoomEvent {
        public UserLeft {
            Objects.requireNonNull(room, "room");
            Objects.requireNonNull(user, "user");
            Objects.requireNonNull(at, "at");
        }
    }

    /** Somebody pinned a ticker. */
    record TickerAdded(String room, RoomTicker ticker, Instant at) implements RoomEvent {
        public TickerAdded {
            Objects.requireNonNull(room, "room");
            Objects.requireNonNull(ticker, "ticker");
            Objects.requireNonNull(at, "at");
        }
    }

    /** Somebody removed their ticker. */
    record TickerRemoved(String room, Username user, Instant at) implements RoomEvent {
        public TickerRemoved {
            Objects.requireNonNull(room, "room");
            Objects.requireNonNull(user, "user");
            Objects.requireNonNull(at, "at");
        }
    }

    /** The server sent the whole ticker list, replacing what we had. */
    record TickerListReceived(String room, List<RoomTicker> tickers, Instant at) implements RoomEvent {
        public TickerListReceived {
            Objects.requireNonNull(room, "room");
            tickers = List.copyOf(Objects.requireNonNull(tickers, "tickers"));
            Objects.requireNonNull(at, "at");
        }
    }

    /** The server sent its room directory. */
    record ListReceived(RoomList list, Instant at) implements RoomEvent {
        public ListReceived {
            Objects.requireNonNull(list, "list");
            Objects.requireNonNull(at, "at");
        }
    }

    /** A message from the all-rooms firehose, for a room we are not in. */
    record PublicChatMessageReceived(String room, Username from, String message, Instant at) implements RoomEvent {
        public PublicChatMessageReceived {
            Objects.requireNonNull(room, "room");
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(message, "message");
            Objects.requireNonNull(at, "at");
        }
    }

    /** We were added to a private room. */
    record MembershipAdded(String room, Instant at) implements RoomEvent {
        public MembershipAdded {
            Objects.requireNonNull(room, "room");
            Objects.requireNonNull(at, "at");
        }
    }

    /** We were removed from a private room. */
    record MembershipRemoved(String room, Instant at) implements RoomEvent {
        public MembershipRemoved {
            Objects.requireNonNull(room, "room");
            Objects.requireNonNull(at, "at");
        }
    }

    /** We were made a moderator of a private room. */
    record ModerationAdded(String room, Instant at) implements RoomEvent {
        public ModerationAdded {
            Objects.requireNonNull(room, "room");
            Objects.requireNonNull(at, "at");
        }
    }

    /** We stopped being a moderator of a private room. */
    record ModerationRemoved(String room, Instant at) implements RoomEvent {
        public ModerationRemoved {
            Objects.requireNonNull(room, "room");
            Objects.requireNonNull(at, "at");
        }
    }
}
