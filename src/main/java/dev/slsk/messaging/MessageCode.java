// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging;

/**
 * Soulseek protocol message codes.
 */
public final class MessageCode {
    private MessageCode() {}

    /** Distributed message codes. */
    public enum Distributed implements ProtocolCode {
        PING(0),
        SEARCH_REQUEST(3),
        BRANCH_LEVEL(4),
        BRANCH_ROOT(5),
        UNKNOWN(6),
        CHILD_DEPTH(7),
        EMBEDDED_MESSAGE(93);

        private final int value;

        Distributed(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }

        @Override
        public int getByteLength() {
            return 1;
        }

        /**
         * Returns the code for a protocol value.
         */
        public static Distributed fromValue(int value) {
            return find(values(), value, "distributed");
        }
    }

    /** SocketConnection initialization codes. */
    public enum Initialization implements ProtocolCode {
        PIERCE_FIREWALL(0),
        PEER_INIT(1);

        private final int value;

        Initialization(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }

        @Override
        public int getByteLength() {
            return 1;
        }

        /**
         * Returns the code for a protocol value.
         */
        public static Initialization fromValue(int value) {
            return find(values(), value, "initialization");
        }
    }

    /** Peer message codes. */
    public enum Peer implements ProtocolCode {
        PRIVATE_MESSAGE(1),
        BROWSE_REQUEST(4),
        BROWSE_RESPONSE(5),
        SEARCH_REQUEST(8),
        SEARCH_RESPONSE(9),
        PRIVATE_ROOM_INVITATION(10),
        CANCELLED_QUEUED_TRANSFER(14),
        INFO_REQUEST(15),
        INFO_RESPONSE(16),
        SEND_CONNECT_TOKEN(33),
        MOVE_DOWNLOAD_TO_TOP(34),
        FOLDER_CONTENTS_REQUEST(36),
        FOLDER_CONTENTS_RESPONSE(37),
        TRANSFER_REQUEST(40),
        TRANSFER_RESPONSE(41),
        UPLOAD_PLACEHOLD(42),
        QUEUE_DOWNLOAD(43),
        PLACE_IN_QUEUE_RESPONSE(44),
        UPLOAD_FAILED(46),
        EXACT_FILE_SEARCH_REQUEST(47),
        QUEUED_DOWNLOADS(48),
        INDIRECT_FILE_SEARCH_REQUEST(49),
        UPLOAD_DENIED(50),
        PLACE_IN_QUEUE_REQUEST(51),
        UPLOAD_QUEUE_NOTIFICATION(52);

        private final int value;

        Peer(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }

        @Override
        public int getByteLength() {
            return 4;
        }

        /**
         * Returns the code for a protocol value.
         */
        public static Peer fromValue(int value) {
            return find(values(), value, "peer");
        }
    }

    /** Server message codes. */
    public enum Server implements ProtocolCode {
        UNKNOWN(0),
        LOGIN(1),
        SET_LISTEN_PORT(2),
        GET_PEER_ADDRESS(3),
        WATCH_USER(5),
        UNWATCH_USER(6),
        GET_STATUS(7),
        SAY_IN_CHAT_ROOM(13),
        JOIN_ROOM(14),
        LEAVE_ROOM(15),
        USER_JOINED_ROOM(16),
        USER_LEFT_ROOM(17),
        CONNECT_TO_PEER(18),
        PRIVATE_MESSAGE(22),
        ACKNOWLEDGE_PRIVATE_MESSAGE(23),
        FILE_SEARCH(26),
        SET_ONLINE_STATUS(28),
        PING(32),
        SEND_SPEED(34),
        SHARED_FOLDERS_AND_FILES(35),
        GET_USER_STATS(36),
        QUEUED_DOWNLOADS(40),
        KICKED_FROM_SERVER(41),
        USER_SEARCH(42),
        INTEREST_ADD(51),
        INTEREST_REMOVE(52),
        GET_RECOMMENDATIONS(54),
        GET_GLOBAL_RECOMMENDATIONS(56),
        GET_USER_INTERESTS(57),
        ROOM_LIST(64),
        EXACT_FILE_SEARCH(65),
        GLOBAL_ADMIN_MESSAGE(66),
        PRIVILEGED_USERS(69),
        HAVE_NO_PARENTS(71),
        PARENTS_IP(73),
        PARENT_MIN_SPEED(83),
        PARENT_SPEED_RATIO(84),
        PARENT_INACTIVITY_TIMEOUT(86),
        SEARCH_INACTIVITY_TIMEOUT(87),
        MINIMUM_PARENTS_IN_CACHE(88),
        DISTRIBUTED_ALIVE_INTERVAL(90),
        ADD_PRIVILEGED_USER(91),
        CHECK_PRIVILEGES(92),
        EMBEDDED_MESSAGE(93),
        ACCEPT_CHILDREN(100),
        NET_INFO(102),
        WISHLIST_SEARCH(103),
        WISHLIST_INTERVAL(104),
        GET_SIMILAR_USERS(110),
        GET_ITEM_RECOMMENDATIONS(111),
        GET_ITEM_SIMILAR_USERS(112),
        ROOM_TICKERS(113),
        ROOM_TICKER_ADD(114),
        ROOM_TICKER_REMOVE(115),
        SET_ROOM_TICKER(116),
        HATED_INTEREST_ADD(117),
        HATED_INTEREST_REMOVE(118),
        ROOM_SEARCH(120),
        SEND_UPLOAD_SPEED(121),
        USER_PRIVILEGES(122),
        GIVE_PRIVILEGES(123),
        NOTIFY_PRIVILEGES(124),
        ACKNOWLEDGE_NOTIFY_PRIVILEGES(125),
        BRANCH_LEVEL(126),
        BRANCH_ROOT(127),
        CHILD_DEPTH(129),
        DISTRIBUTED_RESET(130),
        PRIVATE_ROOM_USERS(133),
        PRIVATE_ROOM_ADD_USER(134),
        PRIVATE_ROOM_REMOVE_USER(135),
        PRIVATE_ROOM_DROP_MEMBERSHIP(136),
        PRIVATE_ROOM_DROP_OWNERSHIP(137),
        PRIVATE_ROOM_UNKNOWN(138),
        PRIVATE_ROOM_ADDED(139),
        PRIVATE_ROOM_REMOVED(140),
        PRIVATE_ROOM_TOGGLE(141),
        NEW_PASSWORD(142),
        PRIVATE_ROOM_ADD_OPERATOR(143),
        PRIVATE_ROOM_REMOVE_OPERATOR(144),
        PRIVATE_ROOM_OPERATOR_ADDED(145),
        PRIVATE_ROOM_OPERATOR_REMOVED(146),
        PRIVATE_ROOM_OWNED(148),
        MESSAGE_USERS(149),
        ASK_PUBLIC_CHAT(150),
        STOP_PUBLIC_CHAT(151),
        PUBLIC_CHAT(152),
        RELATED_SEARCH(153),
        EXCLUDED_SEARCH_PHRASES(160),
        CANNOT_CONNECT(1001),
        CANNOT_CREATE_ROOM(1002),
        CANNOT_JOIN_ROOM(1003);

        private final int value;

        Server(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }

        @Override
        public int getByteLength() {
            return 4;
        }

        /**
         * Returns the code for a protocol value.
         */
        public static Server fromValue(int value) {
            return find(values(), value, "server");
        }
    }

    private static <T extends Enum<T> & ProtocolCode> T find(T[] values, int value, String family) {
        for (T code : values) {
            if (code.getValue() == value) {
                return code;
            }
        }
        throw new IllegalArgumentException("Unknown " + family + " message code: " + value);
    }
}
