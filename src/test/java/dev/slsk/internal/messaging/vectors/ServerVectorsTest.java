// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-FileCopyrightText: aioslsk contributors
// SPDX-FileCopyrightText: Nicotine+ Contributors
// SPDX-License-Identifier: GPL-3.0-only

// GENERATED — edit tools/wire-vectors/ and re-run generate.py. Do not hand-edit.

package dev.slsk.internal.messaging.vectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import dev.slsk.internal.UserPresence;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Wire vectors for the server message family, cross-checked against aioslsk.
 *
 * <p>83 vectors: 43 byte-exact (Tier A, encode) and
 * 40 framing-only (Tier C, decode). Tier assignment and the reason for
 * every demotion are recorded in tools/wire-vectors/bindings.json.</p>
 */
class ServerVectorsTest {
    private static byte[] hex(String value) {
        return WireVectors.hex(value);
    }

    private static InetAddress address(String value) {
        try {
            return InetAddress.getByName(value);
        } catch (UnknownHostException exception) {
            throw new IllegalArgumentException(value, exception);
        }
    }

    @Nested
    @DisplayName("AcceptChildren")
    class AcceptChildrenVectors {

        @Test
        @DisplayName("test_AcceptChildren_Request_deserialize, test_AcceptChildren_Request_serialize")
        void acceptChildren_Request_deserialize() {
            assertArrayEquals(hex("050000006400000001"), new AcceptChildrenCommand(true).toByteArray());
        }
    }

    @Nested
    @DisplayName("AddPrivilegedUser")
    class AddPrivilegedUserVectors {

        @Test
        @DisplayName("test_AddPrivilegedUser_Response_deserialize, test_AddPrivilegedUser_Response_serialize")
        void addPrivilegedUser_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(
                    () -> PrivilegedUserNotification.fromByteArray(hex("0d0000005b000000050000007573657230"))));
        }
    }

    @Nested
    @DisplayName("AddUser")
    class AddUserVectors {

        @Test
        @DisplayName("test_AddUser_Request_deserialize, test_AddUser_Request_serialize")
        void addUser_Request_deserialize() {
            assertArrayEquals(hex("0d00000005000000050000007573657230"), new WatchUserRequest("user0").toByteArray());
        }

        @Test
        @DisplayName("test_AddUser_Response_deserialize_existsWithoutCountryCode,"
                + " test_AddUser_Response_serialize_existsWithoutCountryCode")
        void addUser_Response_deserialize_existsWithoutCountryCode_decodes() {
            assertNotNull(assertDoesNotThrow(() -> WatchUserResponse.fromByteArray(
                    hex("2600000005000000050000007573657230010100000064000000e80300000000000010270000a0860100"))));
        }

        @Test
        @DisplayName("test_AddUser_Response_deserialize_existsWithCountryCode,"
                + " test_AddUser_Response_serialize_existsWithCountryCode")
        void addUser_Response_deserialize_existsWithCountryCode_decodes() {
            assertNotNull(
                    assertDoesNotThrow(
                            () -> WatchUserResponse.fromByteArray(
                                    hex(
                                            "2c00000005000000050000007573657230010100000064000000e80300000000000010270000a0860100020000004445"))));
        }
    }

    @Nested
    @DisplayName("AdminMessage")
    class AdminMessageVectors {

        @Test
        @DisplayName("test_AdminMessage_Response_deserialize, test_AdminMessage_Response_serialize")
        void adminMessage_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(
                    () -> GlobalMessageNotification.fromByteArray(hex("0d000000420000000500000068656c6c6f"))));
        }
    }

    @Nested
    @DisplayName("BranchLevel")
    class BranchLevelVectors {

        @Test
        @DisplayName("test_BranchLevel_Request_deserialize, test_BranchLevel_Request_serialize")
        void branchLevel_Request_deserialize() {
            assertArrayEquals(hex("080000007e00000005000000"), new BranchLevelCommand(5).toByteArray());
        }
    }

    @Nested
    @DisplayName("BranchRoot")
    class BranchRootVectors {

        @Test
        @DisplayName("test_BranchRoot_Request_deserialize, test_BranchRoot_Request_serialize")
        void branchRoot_Request_deserialize() {
            assertArrayEquals(hex("0d0000007f000000050000007573657230"), new BranchRootCommand("user0").toByteArray());
        }
    }

    @Nested
    @DisplayName("CannotConnect")
    class CannotConnectVectors {

        @Test
        @DisplayName("test_CannotConnect_Response_deserialize, test_CannotConnect_Response_serialize")
        void cannotConnect_Response_deserialize() {
            assertArrayEquals(hex("08000000e9030000d2040000"), new CannotConnect(1234).toByteArray());
        }

        @Test
        @DisplayName("test_CannotConnect_Request_deserialize, test_CannotConnect_Request_serialize")
        void cannotConnect_Request_deserialize() {
            assertArrayEquals(
                    hex("11000000e9030000d2040000050000007573657230"), new CannotConnect(1234, "user0").toByteArray());
        }
    }

    @Nested
    @DisplayName("CannotCreateRoom")
    class CannotCreateRoomVectors {

        @Test
        @DisplayName(
                "test_CannotCreateRoom_Response_deserialize_withUsername, test_CannotCreateRoom_Response_serialize")
        void cannotCreateRoom_Response_deserialize_withUsername_decodes() {
            assertNotNull(assertDoesNotThrow(
                    () -> CannotJoinRoomNotification.fromByteArray(hex("0d000000eb03000005000000726f6f6d30"))));
        }
    }

    @Nested
    @DisplayName("CheckPrivileges")
    class CheckPrivilegesVectors {

        @Test
        @DisplayName("test_CheckPrivileges_Request_deserialize, test_CheckPrivileges_Request_serialize")
        void checkPrivileges_Request_deserialize() {
            assertArrayEquals(hex("040000005c000000"), new CheckPrivilegesRequest().toByteArray());
        }

        @Test
        @DisplayName("test_CheckPrivileges_Response_deserialize, test_CheckPrivileges_Response_serialize,"
                + " test_whenDeserializeServerResponse_shouldDeserialize")
        void checkPrivileges_Response_deserialize_decodes() {
            assertDoesNotThrow(
                    () -> IntegerResponse.fromByteArray(hex("080000005c000000e8030000"), MessageCode.Server.class));
        }
    }

    @Nested
    @DisplayName("ChildDepth")
    class ChildDepthVectors {

        @Test
        @DisplayName("test_ChildDepth_Request_deserialize, test_ChildDepth_Request_serialize")
        void childDepth_Request_deserialize() {
            assertArrayEquals(hex("080000008100000005000000"), new ChildDepthCommand(5).toByteArray());
        }
    }

    @Nested
    @DisplayName("ConnectToPeer")
    class ConnectToPeerVectors {

        @Test
        @DisplayName("test_ConnectToPeer_Request_deserialize, test_ConnectToPeer_Request_serialize")
        void connectToPeer_Request_deserialize() {
            assertArrayEquals(
                    hex("1600000012000000d20400000500000075736572300100000050"),
                    new ConnectToPeerRequest(1234, "user0", "P").toByteArray());
        }

        @Test
        @DisplayName("test_ConnectToPeer_Response_deserialize_withoutObfuscatedPort,"
                + " test_ConnectToPeer_Response_serialize_withoutObfuscatedPort")
        void connectToPeer_Response_deserialize_withoutObfuscatedPort_decodes() {
            assertNotNull(assertDoesNotThrow(() -> ConnectToPeerResponse.fromByteArray(
                    hex("1f00000012000000050000007573657230010000005004030201d2040000e803000001"))));
        }

        @Test
        @DisplayName("test_ConnectToPeer_Response_deserialize_emptyObfuscatedPort,"
                + " test_ConnectToPeer_Response_serialize_emptyObfuscatedPort")
        void connectToPeer_Response_deserialize_emptyObfuscatedPort_decodes() {
            assertNotNull(assertDoesNotThrow(() -> ConnectToPeerResponse.fromByteArray(
                    hex("2700000012000000050000007573657230010000005004030201d2040000e8030000010000000000000000"))));
        }

        @Test
        @DisplayName("test_ConnectToPeer_Response_deserialize_withObfuscatedPort,"
                + " test_ConnectToPeer_Response_serialize_withObfuscatedPort")
        void connectToPeer_Response_deserialize_withObfuscatedPort_decodes() {
            assertNotNull(assertDoesNotThrow(() -> ConnectToPeerResponse.fromByteArray(
                    hex("2700000012000000050000007573657230010000005004030201d2040000e80300000101000000d3040000"))));
        }
    }

    @Nested
    @DisplayName("DisablePublicChat")
    class DisablePublicChatVectors {

        @Test
        @DisplayName("test_DisablePublicChat_Request_deserialize, test_DisablePublicChat_Request_serialize")
        void disablePublicChat_Request_deserialize() {
            assertArrayEquals(hex("0400000097000000"), new StopPublicChatCommand().toByteArray());
        }
    }

    @Nested
    @DisplayName("EnablePublicChat")
    class EnablePublicChatVectors {

        @Test
        @DisplayName("test_EnablePublicChat_Request_deserialize, test_EnablePublicChat_Request_serialize")
        void enablePublicChat_Request_deserialize() {
            assertArrayEquals(hex("0400000096000000"), new StartPublicChatCommand().toByteArray());
        }
    }

    @Nested
    @DisplayName("ExcludedSearchPhrases")
    class ExcludedSearchPhrasesVectors {

        @Test
        @DisplayName("test_ExcludedSearchPhrases_Response_deserialize, test_ExcludedSearchPhrases_Response_serialize")
        void excludedSearchPhrases_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() -> ExcludedSearchPhrasesNotification.fromByteArray(
                    hex("19000000a0000000010000000d00000062616e6e656420706872617365"))));
        }
    }

    @Nested
    @DisplayName("FileSearch")
    class FileSearchVectors {

        @Test
        @DisplayName("test_FileSearch_Request_deserialize, test_FileSearch_Request_serialize")
        void fileSearch_Request_deserialize() {
            assertArrayEquals(
                    hex("110000001a000000d2040000050000005175657279"), new SearchRequest("Query", 1234).toByteArray());
        }

        @Test
        @DisplayName("test_FileSearch_Response_deserialize, test_FileSearch_Response_serialize")
        void fileSearch_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() -> ServerSearchRequest.fromByteArray(
                    hex("1a0000001a000000050000007573657230d2040000050000005175657279"))));
        }
    }

    @Nested
    @DisplayName("GetPeerAddress")
    class GetPeerAddressVectors {

        @Test
        @DisplayName("test_GetPeerAddress_Request_deserialize, test_GetPeerAddress_Request_serialize")
        void getPeerAddress_Request_deserialize() {
            assertArrayEquals(hex("0d00000003000000050000007573657230"), new UserAddressRequest("user0").toByteArray());
        }

        @Test
        @DisplayName("test_GetPeerAddress_Response_deserialize_withoutObfuscatedPorts,"
                + " test_GetPeerAddress_Response_serialize_withoutObfuscatedPorts")
        void getPeerAddress_Response_deserialize_withoutObfuscatedPorts_decodes() {
            assertNotNull(assertDoesNotThrow(() ->
                    UserAddressResponse.fromByteArray(hex("150000000300000005000000757365723004030201d2040000"))));
        }

        @Test
        @DisplayName("test_GetPeerAddress_Response_deserialize_withObfuscatedPorts,"
                + " test_GetPeerAddress_Response_serialize_withObfuscatedPorts")
        void getPeerAddress_Response_deserialize_withObfuscatedPorts_decodes() {
            assertNotNull(assertDoesNotThrow(() -> UserAddressResponse.fromByteArray(
                    hex("1b0000000300000005000000757365723004030201d204000001000000d304"))));
        }
    }

    @Nested
    @DisplayName("GetUserPrivileges")
    class GetUserPrivilegesVectors {

        @Test
        @DisplayName("test_GetUserPrivileges_Request_deserialize, test_GetUserPrivileges_Request_serialize")
        void getUserPrivileges_Request_deserialize() {
            assertArrayEquals(
                    hex("0d0000007a000000050000007573657230"), new UserPrivilegesRequest("user0").toByteArray());
        }

        @Test
        @DisplayName("test_GetUserPrivileges_Response_deserialize, test_GetUserPrivileges_Response_serialize")
        void getUserPrivileges_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(
                    () -> UserPrivilegeResponse.fromByteArray(hex("0e0000007a00000005000000757365723001"))));
        }
    }

    @Nested
    @DisplayName("GetUserStats")
    class GetUserStatsVectors {

        @Test
        @DisplayName("test_GetUserStats_Request_deserialize, test_GetUserStats_Request_serialize")
        void getUserStats_Request_deserialize() {
            assertArrayEquals(
                    hex("0d00000024000000050000007573657230"), new UserStatisticsRequest("user0").toByteArray());
        }

        @Test
        @DisplayName("test_GetUserStats_Response_deserialize, test_GetUserStats_Response_serialize")
        void getUserStats_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() -> UserStatisticsResponseFactory.fromByteArray(
                    hex("2100000024000000050000007573657230a086010040420f000000000010270000e8030000"))));
        }
    }

    @Nested
    @DisplayName("GetUserStatus")
    class GetUserStatusVectors {

        @Test
        @DisplayName("test_GetUserStatus_Request_deserialize, test_GetUserStatus_Request_serialize")
        void getUserStatus_Request_deserialize() {
            assertArrayEquals(hex("0d00000007000000050000007573657230"), new UserStatusRequest("user0").toByteArray());
        }

        @Test
        @DisplayName("test_GetUserStatus_Response_deserialize, test_GetUserStatus_Response_serialize")
        void getUserStatus_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() ->
                    UserStatusResponseFactory.fromByteArray(hex("12000000070000000500000075736572300200000001"))));
        }
    }

    @Nested
    @DisplayName("GiveUserPrivileges")
    class GiveUserPrivilegesVectors {

        @Test
        @DisplayName("test_GiveUserPrivileges_Request_deserialize, test_GiveUserPrivileges_Request_serialize")
        void giveUserPrivileges_Request_deserialize() {
            assertArrayEquals(
                    hex("110000007b000000050000007573657230e8030000"),
                    new GivePrivilegesCommand("user0", 1000).toByteArray());
        }
    }

    @Nested
    @DisplayName("LeaveRoom")
    class LeaveRoomVectors {

        @Test
        @DisplayName("test_LeaveRoom_Request_deserialize, test_LeaveRoom_Request_serialize,"
                + " test_LeaveRoom_Response_deserialize, test_LeaveRoom_Response_serialize")
        void leaveRoom_Request_deserialize() {
            assertArrayEquals(hex("0d0000000f00000005000000726f6f6d30"), new LeaveRoomRequest("room0").toByteArray());
        }
    }

    @Nested
    @DisplayName("Login")
    class LoginVectors {

        @Test
        @DisplayName("test_Login_Response_deserialize_unsuccessful, test_Login_Response_serialize_unsuccessful")
        void login_Response_deserialize_unsuccessful_decodes() {
            assertNotNull(assertDoesNotThrow(
                    () -> LoginResponse.fromByteArray(hex("1400000001000000000b000000494e56414c494450415353"))));
        }

        @Test
        @DisplayName("test_Login_Response_deserialize_successful, test_Login_Response_serialize_successful")
        void login_Response_deserialize_successful_decodes() {
            assertNotNull(
                    assertDoesNotThrow(
                            () -> LoginResponse.fromByteArray(
                                    hex(
                                            "3700000001000000010500000048656c6c6f0403020120000000326339333431636134636633643837623965346562393035643661336563343501"))));
        }
    }

    @Nested
    @DisplayName("NewPassword")
    class NewPasswordVectors {

        @Test
        @DisplayName("test_NewPassword_Request_deserialize, test_NewPassword_Request_serialize")
        void newPassword_Request_deserialize() {
            assertArrayEquals(
                    hex("110000008e0000000900000070617373776f726430"), new NewPassword("password0").toByteArray());
        }
    }

    @Nested
    @DisplayName("ParentIP")
    class ParentIPVectors {

        @Test
        @DisplayName("test_ParentIP_Request_deserialize, test_ParentIP_Request_serialize")
        void parentIP_Request_deserialize() {
            assertArrayEquals(hex("080000004900000004030201"), new ParentsIPCommand(address("1.2.3.4")).toByteArray());
        }
    }

    @Nested
    @DisplayName("ParentMinSpeed")
    class ParentMinSpeedVectors {

        @Test
        @DisplayName("test_ParentMinSpeed_Response_deserialize, test_ParentMinSpeed_Response_serialize")
        void parentMinSpeed_Response_deserialize_decodes() {
            assertDoesNotThrow(
                    () -> IntegerResponse.fromByteArray(hex("0800000053000000e8030000"), MessageCode.Server.class));
        }
    }

    @Nested
    @DisplayName("ParentSpeedRatio")
    class ParentSpeedRatioVectors {

        @Test
        @DisplayName("test_ParentSpeedRatio_Response_deserialize, test_ParentSpeedRatio_Response_serialize")
        void parentSpeedRatio_Response_deserialize_decodes() {
            assertDoesNotThrow(
                    () -> IntegerResponse.fromByteArray(hex("0800000054000000e8030000"), MessageCode.Server.class));
        }
    }

    @Nested
    @DisplayName("Ping")
    class PingVectors {

        @Test
        @DisplayName("test_Ping_Request_deserialize, test_Ping_Request_serialize,"
                + " test_whenDeserializeServerRequest_shouldDeserialize")
        void ping_Request_deserialize() {
            assertArrayEquals(hex("0400000020000000"), new ServerPing().toByteArray());
        }
    }

    @Nested
    @DisplayName("PotentialParents")
    class PotentialParentsVectors {

        @Test
        @DisplayName("test_PotentialParents_Response_deserialize, test_PotentialParents_Response_serialize")
        void potentialParents_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() -> NetInfoNotification.fromByteArray(hex(
                    "2a000000660000000200000005000000757365723004030201d204000005000000757365723105030201d3040000"))));
        }
    }

    @Nested
    @DisplayName("PrivateChatMessage")
    class PrivateChatMessageVectors {

        @Test
        @DisplayName("test_PrivateChatMessage_Request_deserialize, test_PrivateChatMessage_Request_serialize")
        void privateChatMessage_Request_deserialize() {
            assertArrayEquals(
                    hex("16000000160000000500000075736572300500000048656c6c6f"),
                    new PrivateMessageCommand("user0", "Hello").toByteArray());
        }

        @Test
        @DisplayName("test_PrivateChatMessage_Response_deserialize_withIsAdmin,"
                + " test_PrivateChatMessage_Response_serialize_withIsAdmin")
        void privateChatMessage_Response_deserialize_withIsAdmin_decodes() {
            assertNotNull(assertDoesNotThrow(() -> PrivateMessageNotification.fromByteArray(
                    hex("1f0000001600000040e20100056556630500000075736572300500000048656c6c6f01"))));
        }
    }

    @Nested
    @DisplayName("PrivateChatMessageAck")
    class PrivateChatMessageAckVectors {

        @Test
        @DisplayName("test_PrivateChatMessageAck_Request_deserialize, test_PrivateChatMessageAck_Request_serialize")
        void privateChatMessageAck_Request_deserialize() {
            assertArrayEquals(
                    hex("0800000017000000d2040000"), new AcknowledgePrivateMessageCommand(1234).toByteArray());
        }
    }

    @Nested
    @DisplayName("PrivateRoomDropMembership")
    class PrivateRoomDropMembershipVectors {

        @Test
        @DisplayName(
                "test_PrivateRoomDropMembership_Request_deserialize, test_PrivateRoomDropMembership_Request_serialize")
        void privateRoomDropMembership_Request_deserialize() {
            assertArrayEquals(
                    hex("0d0000008800000005000000726f6f6d30"),
                    new PrivateRoomDropMembershipCommand("room0").toByteArray());
        }
    }

    @Nested
    @DisplayName("PrivateRoomDropOwnership")
    class PrivateRoomDropOwnershipVectors {

        @Test
        @DisplayName(
                "test_PrivateRoomDropOwnership_Request_deserialize, test_PrivateRoomDropOwnership_Request_serialize")
        void privateRoomDropOwnership_Request_deserialize() {
            assertArrayEquals(
                    hex("0d0000008900000005000000726f6f6d30"),
                    new PrivateRoomDropOwnershipCommand("room0").toByteArray());
        }
    }

    @Nested
    @DisplayName("PrivateRoomGrantMembership")
    class PrivateRoomGrantMembershipVectors {

        @Test
        @DisplayName("test_PrivateRoomGrantMembership_Request_deserialize,"
                + " test_PrivateRoomGrantMembership_Request_serialize,"
                + " test_PrivateRoomGrantMembership_Response_deserialize,"
                + " test_PrivateRoomGrantMembership_Response_serialize")
        void privateRoomGrantMembership_Request_deserialize() {
            assertArrayEquals(
                    hex("160000008600000005000000726f6f6d30050000007573657230"),
                    new PrivateRoomAddUser("room0", "user0").toByteArray());
        }
    }

    @Nested
    @DisplayName("PrivateRoomGrantOperator")
    class PrivateRoomGrantOperatorVectors {

        @Test
        @DisplayName(
                "test_PrivateRoomGrantOperator_Request_deserialize, test_PrivateRoomGrantOperator_Request_serialize,"
                        + " test_PrivateRoomGrantOperator_Response_deserialize,"
                        + " test_PrivateRoomGrantOperator_Response_serialize")
        void privateRoomGrantOperator_Request_deserialize() {
            assertArrayEquals(
                    hex("160000008f00000005000000726f6f6d30050000007573657230"),
                    new PrivateRoomAddOperator("room0", "user0").toByteArray());
        }
    }

    @Nested
    @DisplayName("PrivateRoomMembers")
    class PrivateRoomMembersVectors {

        @Test
        @DisplayName("test_PrivateRoomMembers_Response_deserialize, test_PrivateRoomMembers_Response_serialize")
        void privateRoomMembers_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() -> PrivateRoomUserListNotification.fromByteArray(
                    hex("230000008500000005000000726f6f6d3002000000050000007573657230050000007573657231"))));
        }
    }

    @Nested
    @DisplayName("PrivateRoomMembershipGranted")
    class PrivateRoomMembershipGrantedVectors {

        @Test
        @DisplayName("test_PrivateRoomMembershipGranted_Response_deserialize,"
                + " test_PrivateRoomMembershipGranted_Response_serialize")
        void privateRoomMembershipGranted_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() ->
                    StringResponse.fromByteArray(hex("0d0000008b00000005000000726f6f6d30"), MessageCode.Server.class)));
        }
    }

    @Nested
    @DisplayName("PrivateRoomMembershipRevoked")
    class PrivateRoomMembershipRevokedVectors {

        @Test
        @DisplayName("test_PrivateRoomMembershipRevoked_Response_deserialize,"
                + " test_PrivateRoomMembershipRevoked_Response_serialize")
        void privateRoomMembershipRevoked_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() ->
                    StringResponse.fromByteArray(hex("0d0000008c00000005000000726f6f6d30"), MessageCode.Server.class)));
        }
    }

    @Nested
    @DisplayName("PrivateRoomOperatorGranted")
    class PrivateRoomOperatorGrantedVectors {

        @Test
        @DisplayName("test_PrivateRoomOperatorGranted_Response_deserialize,"
                + " test_PrivateRoomOperatorGranted_Response_serialize")
        void privateRoomOperatorGranted_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() ->
                    StringResponse.fromByteArray(hex("0d0000009100000005000000726f6f6d30"), MessageCode.Server.class)));
        }
    }

    @Nested
    @DisplayName("PrivateRoomOperatorRevoked")
    class PrivateRoomOperatorRevokedVectors {

        @Test
        @DisplayName("test_PrivateRoomOperatorRevoked_Response_deserialize,"
                + " test_PrivateRoomOperatorRevoked_Response_serialize")
        void privateRoomOperatorRevoked_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() ->
                    StringResponse.fromByteArray(hex("0d0000009200000005000000726f6f6d30"), MessageCode.Server.class)));
        }
    }

    @Nested
    @DisplayName("PrivateRoomOperators")
    class PrivateRoomOperatorsVectors {

        @Test
        @DisplayName("test_PrivateRoomOperators_Response_deserialize, test_PrivateRoomOperators_Response_serialize")
        void privateRoomOperators_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() -> PrivateRoomOwnedListNotification.fromByteArray(
                    hex("230000009400000005000000726f6f6d3002000000050000007573657230050000007573657231"))));
        }
    }

    @Nested
    @DisplayName("PrivateRoomRevokeMembership")
    class PrivateRoomRevokeMembershipVectors {

        @Test
        @DisplayName("test_PrivateRoomRevokeMembership_Request_deserialize,"
                + " test_PrivateRoomRevokeMembership_Request_serialize,"
                + " test_PrivateRoomRevokeMembership_Response_deserialize,"
                + " test_PrivateRoomRevokeMembership_Response_serialize")
        void privateRoomRevokeMembership_Request_deserialize() {
            assertArrayEquals(
                    hex("160000008700000005000000726f6f6d30050000007573657230"),
                    new PrivateRoomRemoveUser("room0", "user0").toByteArray());
        }
    }

    @Nested
    @DisplayName("PrivateRoomRevokeOperator")
    class PrivateRoomRevokeOperatorVectors {

        @Test
        @DisplayName(
                "test_PrivateRoomRevokeOperator_Request_deserialize, test_PrivateRoomRevokeOperator_Request_serialize,"
                        + " test_PrivateRoomRevokeOperator_Response_deserialize,"
                        + " test_PrivateRoomRevokeOperator_Response_serialize")
        void privateRoomRevokeOperator_Request_deserialize() {
            assertArrayEquals(
                    hex("160000009000000005000000726f6f6d30050000007573657230"),
                    new PrivateRoomRemoveOperator("room0", "user0").toByteArray());
        }
    }

    @Nested
    @DisplayName("PrivilegedUsers")
    class PrivilegedUsersVectors {

        @Test
        @DisplayName("test_PrivilegedUsers_Response_deserialize, test_PrivilegedUsers_Response_serialize")
        void privilegedUsers_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() -> PrivilegedUserListNotification.fromByteArray(
                    hex("1a0000004500000002000000050000007573657230050000007573657231"))));
        }
    }

    @Nested
    @DisplayName("PrivilegesNotification")
    class PrivilegesNotificationVectors {

        @Test
        @DisplayName("test_PrivilegesNotification_Request_deserialize, test_PrivilegesNotification_Request_serialize")
        void privilegesNotification_Request_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(
                    () -> PrivilegeNotification.fromByteArray(hex("110000007c000000e8030000050000007573657230"))));
        }
    }

    @Nested
    @DisplayName("PrivilegesNotificationAck")
    class PrivilegesNotificationAckVectors {

        @Test
        @DisplayName(
                "test_PrivilegesNotificationAck_Request_deserialize, test_PrivilegesNotificationAck_Request_serialize")
        void privilegesNotificationAck_Request_deserialize() {
            assertArrayEquals(
                    hex("080000007d000000e8030000"), new AcknowledgePrivilegeNotificationCommand(1000).toByteArray());
        }
    }

    @Nested
    @DisplayName("PublicChatMessage")
    class PublicChatMessageVectors {

        @Test
        @DisplayName("test_PublicChatMessage_Response_deserialize, test_PublicChatMessage_Response_serialize")
        void publicChatMessage_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() -> PublicChatMessageNotification.fromByteArray(
                    hex("1f0000009800000005000000726f6f6d300500000075736572300500000048656c6c6f"))));
        }
    }

    @Nested
    @DisplayName("RemoveUser")
    class RemoveUserVectors {

        @Test
        @DisplayName("test_RemoveUser_Request_deserialize, test_RemoveUser_Request_serialize")
        void removeUser_Request_deserialize() {
            assertArrayEquals(hex("0d00000006000000050000007573657230"), new UnwatchUserCommand("user0").toByteArray());
        }
    }

    @Nested
    @DisplayName("RoomChatMessage")
    class RoomChatMessageVectors {

        @Test
        @DisplayName("test_RoomChatMessage_Request_deserialize, test_RoomChatMessage_Request_serialize")
        void roomChatMessage_Request_deserialize() {
            assertArrayEquals(
                    hex("160000000d00000005000000726f6f6d300500000048656c6c6f"),
                    new RoomMessageCommand("room0", "Hello").toByteArray());
        }

        @Test
        @DisplayName("test_RoomChatMessage_Response_deserialize, test_RoomChatMessage_Response_serialize")
        void roomChatMessage_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() -> RoomMessageNotification.fromByteArray(
                    hex("1f0000000d00000005000000726f6f6d300500000075736572300500000048656c6c6f"))));
        }
    }

    @Nested
    @DisplayName("RoomList")
    class RoomListVectors {

        @Test
        @DisplayName("test_RoomList_Request_deserialize, test_RoomList_Request_serialize")
        void roomList_Request_deserialize() {
            assertArrayEquals(hex("0400000040000000"), new RoomListRequest().toByteArray());
        }
    }

    @Nested
    @DisplayName("RoomSearch")
    class RoomSearchVectors {

        @Test
        @DisplayName("test_RoomSearch_Request_deserialize, test_RoomSearch_Request_serialize")
        void roomSearch_Request_deserialize() {
            assertArrayEquals(
                    hex("1a0000007800000005000000726f6f6d30d2040000050000005175657279"),
                    new RoomSearchRequest("room0", "Query", 1234).toByteArray());
        }
    }

    @Nested
    @DisplayName("RoomTickerAdded")
    class RoomTickerAddedVectors {

        @Test
        @DisplayName("test_RoomTickerAdded_Response_deserialize, test_RoomTickerAdded_Response_serialize")
        void roomTickerAdded_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() -> RoomTickerAddedNotification.fromByteArray(
                    hex("210000007200000005000000726f6f6d30050000007573657230070000007469636b657230"))));
        }
    }

    @Nested
    @DisplayName("RoomTickerRemoved")
    class RoomTickerRemovedVectors {

        @Test
        @DisplayName("test_RoomTickerRemoved_Response_deserialize, test_RoomTickerRemoved_Response_serialize")
        void roomTickerRemoved_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() -> RoomTickerRemovedNotification.fromByteArray(
                    hex("160000007300000005000000726f6f6d30050000007573657230"))));
        }
    }

    @Nested
    @DisplayName("RoomTickers")
    class RoomTickersVectors {

        @Test
        @DisplayName("test_RoomTickers_Response_deserialize, test_RoomTickers_Response_serialize")
        void roomTickers_Response_deserialize_decodes() {
            assertNotNull(
                    assertDoesNotThrow(
                            () -> RoomTickerListNotification.fromByteArray(
                                    hex(
                                            "390000007100000005000000726f6f6d3002000000050000007573657230070000007469636b657230050000007573657231070000007469636b657231"))));
        }
    }

    @Nested
    @DisplayName("SendUploadSpeed")
    class SendUploadSpeedVectors {

        @Test
        @DisplayName("test_SendUploadSpeed_Request_deserialize, test_SendUploadSpeed_Request_serialize")
        void sendUploadSpeed_Request_deserialize() {
            assertArrayEquals(hex("0800000079000000e8030000"), new SendUploadSpeedCommand(1000).toByteArray());
        }
    }

    @Nested
    @DisplayName("ServerSearchRequest")
    class ServerSearchRequestVectors {

        @Test
        @DisplayName("test_ServerSearchRequest_Response_deserialize, test_ServerSearchRequest_Response_serialize")
        void serverSearchRequest_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() -> EmbeddedMessage.fromByteArray(
                    hex("1f0000005d0000000300000000050000007573657230d2040000050000005175657279"))));
        }
    }

    @Nested
    @DisplayName("SetListenPort")
    class SetListenPortVectors {

        @Test
        @DisplayName("test_SetListenPort_Request_deserialize_withoutObfuscatedPorts,"
                + " test_SetListenPort_Request_serialize_withoutObfuscatedPorts")
        void setListenPort_Request_deserialize_withoutObfuscatedPorts() {
            assertArrayEquals(hex("0800000002000000d2040000"), new SetListenPortCommand(1234).toByteArray());
        }
    }

    @Nested
    @DisplayName("SetRoomTicker")
    class SetRoomTickerVectors {

        @Test
        @DisplayName("test_SetRoomTicker_Request_deserialize, test_SetRoomTicker_Request_serialize")
        void setRoomTicker_Request_deserialize() {
            assertArrayEquals(
                    hex("180000007400000005000000726f6f6d30070000007469636b657230"),
                    new SetRoomTickerCommand("room0", "ticker0").toByteArray());
        }
    }

    @Nested
    @DisplayName("SetStatus")
    class SetStatusVectors {

        @Test
        @DisplayName("test_SetStatus_Request_deserialize, test_SetStatus_Request_serialize")
        void setStatus_Request_deserialize() {
            assertArrayEquals(
                    hex("080000001c00000002000000"), new SetOnlineStatusCommand(UserPresence.ONLINE).toByteArray());
        }
    }

    @Nested
    @DisplayName("SharedFoldersFiles")
    class SharedFoldersFilesVectors {

        @Test
        @DisplayName("test_SharedFoldersFiles_Request_deserialize, test_SharedFoldersFiles_Request_serialize")
        void sharedFoldersFiles_Request_deserialize() {
            assertArrayEquals(
                    hex("0c00000023000000e803000010270000"), new SetSharedCountsCommand(1000, 10000).toByteArray());
        }
    }

    @Nested
    @DisplayName("ToggleParentSearch")
    class ToggleParentSearchVectors {

        @Test
        @DisplayName("test_ToggleParentSearch_Request_deserialize, test_ToggleParentSearch_Request_serialize")
        void toggleParentSearch_Request_deserialize() {
            assertArrayEquals(hex("050000004700000001"), new HaveNoParentsCommand(true).toByteArray());
        }
    }

    @Nested
    @DisplayName("TogglePrivateRoomInvites")
    class TogglePrivateRoomInvitesVectors {

        @Test
        @DisplayName(
                "test_TogglePrivateRoomInvites_Request_deserialize, test_TogglePrivateRoomInvites_Request_serialize")
        void togglePrivateRoomInvites_Request_deserialize() {
            assertArrayEquals(hex("050000008d00000001"), new PrivateRoomToggle(true).toByteArray());
        }
    }

    @Nested
    @DisplayName("UserJoinedRoom")
    class UserJoinedRoomVectors {

        @Test
        @DisplayName("test_UserJoinedRoom_Response_deserialize, test_UserJoinedRoom_Response_serialize")
        void userJoinedRoom_Response_deserialize_decodes() {
            assertNotNull(
                    assertDoesNotThrow(
                            () -> UserJoinedRoomNotification.fromByteArray(
                                    hex(
                                            "380000001000000005000000726f6f6d3005000000757365723001000000e80300001027000000000000e8030000e803000005000000020000004445"))));
        }
    }

    @Nested
    @DisplayName("UserLeftRoom")
    class UserLeftRoomVectors {

        @Test
        @DisplayName("test_UserLeftRoom_Response_deserialize, test_UserLeftRoom_Response_serialize")
        void userLeftRoom_Response_deserialize_decodes() {
            assertNotNull(assertDoesNotThrow(() -> UserLeftRoomNotification.fromByteArray(
                    hex("160000001100000005000000726f6f6d30050000007573657230"))));
        }
    }

    @Nested
    @DisplayName("UserSearch")
    class UserSearchVectors {

        @Test
        @DisplayName("test_UserSearch_Request_deserialize, test_UserSearch_Request_serialize")
        void userSearch_Request_deserialize() {
            assertArrayEquals(
                    hex("1a0000002a000000050000007573657230d2040000050000005175657279"),
                    new UserSearchRequest("user0", "Query", 1234).toByteArray());
        }
    }

    @Nested
    @DisplayName("WishlistInterval")
    class WishlistIntervalVectors {

        @Test
        @DisplayName("test_WishlistInterval_Response_deserialize, test_WishlistInterval_Response_serialize")
        void wishlistInterval_Response_deserialize_decodes() {
            assertDoesNotThrow(
                    () -> IntegerResponse.fromByteArray(hex("0800000068000000e8030000"), MessageCode.Server.class));
        }
    }

    @Nested
    @DisplayName("WishlistSearch")
    class WishlistSearchVectors {

        @Test
        @DisplayName("test_WishlistSearch_Request_deserialize, test_WishlistSearch_Request_serialize")
        void wishlistSearch_Request_deserialize() {
            assertArrayEquals(
                    hex("1100000067000000d2040000050000005175657279"),
                    new WishlistSearchRequest("Query", 1234).toByteArray());
        }
    }
}
