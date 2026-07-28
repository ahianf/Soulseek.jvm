// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Attachment;
import dev.slsk.CancellationSignal;
import dev.slsk.EventStream;
import dev.slsk.PrivateRooms;
import dev.slsk.Room;
import dev.slsk.RoomInfo;
import dev.slsk.RoomList;
import dev.slsk.RoomTicker;
import dev.slsk.RoomUser;
import dev.slsk.Rooms;
import dev.slsk.UserPresence;
import dev.slsk.UserStatistics;
import dev.slsk.Username;
import dev.slsk.events.RoomEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * {@link Rooms} over the {@link SoulseekClient} seam.
 *
 * <p>This holds room state, which the old surface did not. Previously a consumer
 * wanting to render "who is in this room" subscribed to joined, user-joined,
 * user-left and the ticker events, and maintained the membership itself — every
 * consumer writing the same reducer, and every one of them wrong after a
 * missed event, because there was nothing to reconcile against.
 *
 * <p>Here the reducer is written once and {@link #get} answers from it. Every
 * mutation goes through the bus so that a snapshot taken by {@link #attach}
 * cannot interleave with one, which is what makes the initial render exact.
 *
 * <p>Membership and tickers are kept. Messages are not, per the rule that the
 * library owns state and the application owns history.
 */
final class DefaultRooms implements Rooms {

    private final SoulseekClient client;
    private final EventBus<RoomEvent> events;
    private final Map<String, Room> rooms = new ConcurrentHashMap<>();
    private final PrivateRooms privateRooms;

    DefaultRooms(SoulseekClient client, EventBus<RoomEvent> events) {
        this.client = Objects.requireNonNull(client, "client");
        this.events = Objects.requireNonNull(events, "events");
        this.privateRooms = new DefaultPrivateRooms(client);
        wire();
    }

    private void wire() {
        client.addRoomMessageReceivedListener((sender, event) -> {
            if (event != null) {
                events.publish(new RoomEvent.MessageReceived(
                        event.getRoomName(), Username.of(event.getUsername()),
                        event.getMessage(), Instant.now()));
            }
        });
        client.addRoomJoinedListener((sender, event) -> {
            if (event == null) {
                return;
            }
            events.mutateAndPublish(() -> {
                Room room = rooms.compute(
                        event.getRoomName(),
                        (name, existing) -> withUser(existing == null ? blank(name) : existing, roomUser(event)));
                return new RoomEvent.UserJoined(event.getRoomName(), roomUser(event), Instant.now());
            });
        });
        client.addRoomLeftListener((sender, event) -> {
            if (event == null) {
                return;
            }
            events.mutateAndPublish(() -> {
                Username user = Username.of(event.getUsername());
                rooms.computeIfPresent(event.getRoomName(), (name, existing) -> withoutUser(existing, user));
                return new RoomEvent.UserLeft(event.getRoomName(), user, Instant.now());
            });
        });
        client.addRoomTickerListReceivedListener((sender, event) -> {
            if (event == null) {
                return;
            }
            events.mutateAndPublish(() -> {
                List<RoomTicker> tickers = tickers(event.getTickers());
                rooms.computeIfPresent(event.getRoomName(), (name, existing) -> withTickers(existing, tickers));
                return new RoomEvent.TickerListReceived(event.getRoomName(), tickers, Instant.now());
            });
        });
        client.addRoomTickerAddedListener((sender, event) -> {
            if (event == null || event.getTicker() == null) {
                return;
            }
            events.mutateAndPublish(() -> {
                RoomTicker ticker = ticker(event.getTicker());
                rooms.computeIfPresent(event.getRoomName(), (name, existing) -> withTicker(existing, ticker));
                return new RoomEvent.TickerAdded(event.getRoomName(), ticker, Instant.now());
            });
        });
        client.addRoomTickerRemovedListener((sender, event) -> {
            if (event == null) {
                return;
            }
            events.mutateAndPublish(() -> {
                Username user = Username.of(event.getUsername());
                rooms.computeIfPresent(event.getRoomName(), (name, existing) -> withoutTicker(existing, user));
                return new RoomEvent.TickerRemoved(event.getRoomName(), user, Instant.now());
            });
        });
        client.addPublicChatMessageReceivedListener((sender, event) -> {
            if (event != null) {
                events.publish(new RoomEvent.PublicChatMessageReceived(
                        event.getRoomName(), Username.of(event.getUsername()),
                        event.getMessage(), Instant.now()));
            }
        });
        client.addRoomListReceivedListener(
                (sender, list) -> events.publish(new RoomEvent.ListReceived(roomList(list), Instant.now())));
        client.addPrivateRoomMembershipAddedListener(
                (sender, room) -> events.publish(new RoomEvent.MembershipAdded(String.valueOf(room), Instant.now())));
        client.addPrivateRoomMembershipRemovedListener(
                (sender, room) -> events.publish(new RoomEvent.MembershipRemoved(String.valueOf(room), Instant.now())));
        client.addPrivateRoomModerationAddedListener(
                (sender, room) -> events.publish(new RoomEvent.ModerationAdded(String.valueOf(room), Instant.now())));
        client.addPrivateRoomModerationRemovedListener(
                (sender, room) -> events.publish(new RoomEvent.ModerationRemoved(String.valueOf(room), Instant.now())));
    }

    // --- state reducers ----------------------------------------------------

    private static Room blank(String name) {
        return new Room(name, List.of(), List.of(), false, Optional.empty(), Set.of());
    }

    private static Room withUser(Room room, RoomUser user) {
        List<RoomUser> users = new ArrayList<>(room.users().stream()
                .filter(existing -> !existing.user().equals(user.user()))
                .toList());
        users.add(user);
        return new Room(room.name(), users, room.tickers(), room.isPrivate(), room.owner(), room.operators());
    }

    private static Room withoutUser(Room room, Username user) {
        return new Room(
                room.name(),
                room.users().stream()
                        .filter(existing -> !existing.user().equals(user))
                        .toList(),
                room.tickers(),
                room.isPrivate(),
                room.owner(),
                room.operators());
    }

    private static Room withTickers(Room room, List<RoomTicker> tickers) {
        return new Room(room.name(), room.users(), tickers, room.isPrivate(), room.owner(), room.operators());
    }

    /** One ticker per user: a second from the same user replaces the first. */
    private static Room withTicker(Room room, RoomTicker ticker) {
        List<RoomTicker> tickers = new ArrayList<>(room.tickers().stream()
                .filter(existing -> !existing.user().equals(ticker.user()))
                .toList());
        tickers.add(ticker);
        return withTickers(room, tickers);
    }

    private static Room withoutTicker(Room room, Username user) {
        return withTickers(
                room,
                room.tickers().stream()
                        .filter(existing -> !existing.user().equals(user))
                        .toList());
    }

    // --- translation -------------------------------------------------------

    private static RoomTicker ticker(dev.slsk.internal.RoomTicker source) {
        return new RoomTicker(Username.of(source.getUsername()), source.getMessage());
    }

    private static List<RoomTicker> tickers(List<dev.slsk.internal.RoomTicker> source) {
        return source == null
                ? List.of()
                : source.stream().map(DefaultRooms::ticker).toList();
    }

    private static RoomUser roomUser(dev.slsk.internal.events.RoomJoinedEvent event) {
        return user(Username.of(event.getUsername()), event.getUserData());
    }

    private static RoomUser user(Username name, dev.slsk.internal.UserData data) {
        if (data == null) {
            return new RoomUser(
                    name,
                    UserPresence.ONLINE,
                    new UserStatistics(name, 0, 0, 0, 0),
                    OptionalInt.empty(),
                    Optional.empty());
        }
        return new RoomUser(
                name,
                data.getStatus() == null
                        ? UserPresence.ONLINE
                        : switch (data.getStatus()) {
                            case OFFLINE -> UserPresence.OFFLINE;
                            case AWAY -> UserPresence.AWAY;
                            case ONLINE -> UserPresence.ONLINE;
                        },
                new UserStatistics(
                        name,
                        data.getAverageSpeed(),
                        data.getUploadCount(),
                        data.getFileCount(),
                        data.getDirectoryCount()),
                data.getSlotsFree() == null ? OptionalInt.empty() : OptionalInt.of(data.getSlotsFree()),
                Optional.ofNullable(data.getCountryCode()));
    }

    private static Room room(dev.slsk.internal.RoomData data) {
        if (data == null) {
            return blank("");
        }
        List<RoomUser> users = data.getUsers() == null
                ? List.of()
                : data.getUsers().stream()
                        .filter(entry -> entry != null && entry.getUsername() != null)
                        .map(entry -> user(Username.of(entry.getUsername()), entry))
                        .toList();
        Set<Username> operators = data.getOperators() == null
                ? Set.of()
                : data.getOperators().stream().map(Username::of).collect(Collectors.toUnmodifiableSet());
        return new Room(
                data.getName(),
                users,
                List.of(),
                data.isPrivate(),
                data.getOwner() == null || data.getOwner().isBlank()
                        ? Optional.empty()
                        : Optional.of(Username.of(data.getOwner())),
                operators);
    }

    private static RoomList roomList(dev.slsk.internal.RoomList source) {
        if (source == null) {
            return RoomList.empty();
        }
        return new RoomList(
                infos(source.getPublic()),
                infos(source.getPrivate()),
                infos(source.getOwned()),
                source.getModeratedRoomNames() == null ? List.of() : List.copyOf(source.getModeratedRoomNames()));
    }

    private static List<RoomInfo> infos(List<dev.slsk.internal.RoomInfo> source) {
        return source == null
                ? List.of()
                : source.stream()
                        .map(info -> new RoomInfo(info.getName(), info.getUserCount()))
                        .toList();
    }

    // --- operations --------------------------------------------------------

    @Override
    public RoomList list(CancellationSignal signal) {
        Objects.requireNonNull(signal, "signal");
        return roomList(client.getRoomList(signal));
    }

    @Override
    public Room join(String name, CancellationSignal signal) {
        Objects.requireNonNull(name, "room");
        Objects.requireNonNull(signal, "signal");
        Room joined = room(client.joinRoom(name, signal));
        events.mutateAndPublish(() -> {
            rooms.put(name, joined);
            return new RoomEvent.Joined(name, joined, Instant.now());
        });
        return joined;
    }

    @Override
    public void leave(String name) {
        Objects.requireNonNull(name, "room");
        if (rooms.containsKey(name)) {
            client.leaveRoom(name);
            events.mutateAndPublish(() -> {
                rooms.remove(name);
                return new RoomEvent.Left(name, Instant.now());
            });
        }
    }

    @Override
    public void say(String name, String message) {
        Objects.requireNonNull(name, "room");
        Objects.requireNonNull(message, "message");
        client.sendRoomMessage(name, message);
    }

    @Override
    public void setTicker(String name, String message) {
        Objects.requireNonNull(name, "room");
        Objects.requireNonNull(message, "message");
        client.setRoomTicker(name, message);
    }

    @Override
    public Room get(String name) {
        Objects.requireNonNull(name, "room");
        Room room = rooms.get(name);
        if (room == null) {
            throw new IllegalArgumentException("not in room: " + name);
        }
        return room;
    }

    @Override
    public List<Room> joined() {
        return List.copyOf(new LinkedHashMap<>(rooms).values());
    }

    @Override
    public void startPublicChat() {
        client.startPublicChat();
    }

    @Override
    public void stopPublicChat() {
        client.stopPublicChat();
    }

    @Override
    public PrivateRooms privateRooms() {
        return privateRooms;
    }

    @Override
    public EventStream<RoomEvent> events() {
        return events;
    }

    @Override
    public Attachment<List<Room>> attach(Consumer<RoomEvent> listener) {
        return events.attach(this::joined, listener);
    }

    /** {@link PrivateRooms} over the same seam. */
    private record DefaultPrivateRooms(SoulseekClient client) implements PrivateRooms {

        @Override
        public void addMember(String room, Username user, CancellationSignal signal) {
            client.addPrivateRoomMember(require(room), user.value(), require(signal));
        }

        @Override
        public void removeMember(String room, Username user, CancellationSignal signal) {
            client.removePrivateRoomMember(require(room), user.value(), require(signal));
        }

        @Override
        public void addOperator(String room, Username user, CancellationSignal signal) {
            client.addPrivateRoomModerator(require(room), user.value(), require(signal));
        }

        @Override
        public void removeOperator(String room, Username user, CancellationSignal signal) {
            client.removePrivateRoomModerator(require(room), user.value(), require(signal));
        }

        @Override
        public void dropMembership(String room, CancellationSignal signal) {
            client.dropPrivateRoomMembership(require(room), require(signal));
        }

        @Override
        public void dropOwnership(String room, CancellationSignal signal) {
            client.dropPrivateRoomOwnership(require(room), require(signal));
        }

        private static <T> T require(T value) {
            return Objects.requireNonNull(value);
        }
    }
}
