// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-FileCopyrightText: aioslsk contributors
// SPDX-FileCopyrightText: Nicotine+ Contributors
// SPDX-License-Identifier: GPL-3.0-only

// GENERATED — edit tools/wire-vectors/ and re-run generate.py. Do not hand-edit.

package dev.slsk.internal.messaging.vectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import dev.slsk.internal.TransferDirection;
import dev.slsk.internal.UserPresence;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.*;

/**
 * Wire vectors for the peer message family, cross-checked against aioslsk.
 *
 * <p>27 vectors: 10 byte-exact (Tier A, encode) and
 * 17 framing-only (Tier C, decode). Tier assignment and the reason for
 * every demotion are recorded in tools/wire-vectors/bindings.json.</p>
 */
class PeerVectorsTest {
    private static byte[] hex(String value) {
        return WireVectors.hex(value);
    }

    @Nested
    @DisplayName("PeerDirectoryContentsReply")
    class PeerDirectoryContentsReplyVectors {

        @Test
        @DisplayName("test_PeerDirectoryContentsReply_Request_deserialize[A\\x00\\x00\\x00%\\x00\\x00\\x00x\\x9c\\xbb\\xc4\\xc2\\xc0\\xc0\\xce\\xc0\\xc0\\xe0l\\x15\\x93\\x92Yd\\xc0\\xc8\\x80\\xc1e\\xe4\\x04\\x12\\xc5\\xf9y\\xe9\\x06z\\xb9\\x05\\xc6\\x0eN\\xfc\\x0c \\xc0\\x0c\\xc4@.\\x13H\\x01\\x10;0\\x82E\\x19^\\x00\\xc5\\x01#\\x1c\\x0by], test_PeerDirectoryContentsReply_Request_serialize")
        void peerDirectoryContentsReply_Request_deserialize_A_x00_x00_x00__x00_x00_x00x_x9c_xbb_xc4_xc2_xc0_xc0_xce_xc0_xc0_xe0l_x15_x93_x92Yd_xc0_xc8_x80_xc1e_xe4_x04_x12_xc5_xf9y_xe9_x06z_xb9_x05_xc6_x0eN_xfc_x0c__xc0_x0c_xc4___x13H_x01_x10_0_x82E_x19__x00_xc5_x01__x1c_x0by__decodes() {
            assertNotNull(assertDoesNotThrow(() -> FolderContentsResponse.fromByteArray(hex("4100000025000000789cbbc4c2c0c0cec0c0e06c1593925964c0c880c165e40412c5f979e9067ab905c60e4efc0c20c00cc4402e134801103b308245195e00c501231c0b79"))));
        }

        @Test
        @DisplayName("test_PeerDirectoryContentsReply_Request_deserialize[I\\x00\\x00\\x00%\\x00\\x00\\x00x\\x9c\\xbb\\xc4\\xc2\\xc0\\xc0\\xce\\xc0\\xc0\\xe0l\\x15\\x93\\x92Yd\\xc0\\xc8\\x80\\xc1e\\xe4d``(\\xce\\xcfK7\\xd0\\xcb-0vp\\xe2g\\x00\\x01f\\x06\\x06\\x86\\xdc\\x02c&\\x90\\x02\\x06\\x06\\x06\\x07\\x10\\xc1\\xc0\\xc0\\xf0\\x82\\x99\\x81\\x01\\x00#\\x1c\\x0by]")
        void peerDirectoryContentsReply_Request_deserialize_I_x00_x00_x00__x00_x00_x00x_x9c_xbb_xc4_xc2_xc0_xc0_xce_xc0_xc0_xe0l_x15_x93_x92Yd_xc0_xc8_x80_xc1e_xe4d____xce_xcfK7_xd0_xcb_0vp_xe2g_x00_x01f_x06_x06_x86_xdc_x02c__x90_x02_x06_x06_x06_x07_x10_xc1_xc0_xc0_xf0_x82_x99_x81_x01_x00__x1c_x0by__decodes() {
            assertNotNull(assertDoesNotThrow(() -> FolderContentsResponse.fromByteArray(hex("4900000025000000789cbbc4c2c0c0cec0c0e06c1593925964c0c880c165e464606028cecf4b37d0cb2d307670e267000166060686dc02632690020606060710c1c0c0f08299810100231c0b79"))));
        }
    }

    @Nested
    @DisplayName("PeerDirectoryContentsRequest")
    class PeerDirectoryContentsRequestVectors {

        @Test
        @DisplayName("test_PeerDirectoryContentsRequest_Request_deserialize, test_PeerDirectoryContentsRequest_Request_serialize")
        void peerDirectoryContentsRequest_Request_deserialize() {
            assertArrayEquals(hex("1300000024000000d204000007000000433a5c64697230"), new FolderContentsRequest(1234, "C:\\dir0").toByteArray());
        }
    }

    @Nested
    @DisplayName("PeerPlaceInQueueReply")
    class PeerPlaceInQueueReplyVectors {

        @Test
        @DisplayName("test_PeerPlaceInQueueReply_Request_deserialize, test_PeerPlaceInQueueReply_Request_serialize")
        void peerPlaceInQueueReply_Request_deserialize() {
            assertArrayEquals(hex("1d0000002c00000011000000433a5c646972305c736f6e67302e6d70330a000000"), new PlaceInQueueResponse("C:\\dir0\\song0.mp3", 10).toByteArray());
        }
    }

    @Nested
    @DisplayName("PeerPlaceInQueueRequest")
    class PeerPlaceInQueueRequestVectors {

        @Test
        @DisplayName("test_PeerPlaceInQueueRequest_Request_deserialize, test_PeerPlaceInQueueRequest_Request_serialize")
        void peerPlaceInQueueRequest_Request_deserialize() {
            assertArrayEquals(hex("190000003300000011000000433a5c646972305c736f6e67302e6d7033"), new PlaceInQueueRequest("C:\\dir0\\song0.mp3").toByteArray());
        }
    }

    @Nested
    @DisplayName("PeerSearchReply")
    class PeerSearchReplyVectors {

        @Test
        @DisplayName("test_PeerSearchReply_Request_deserialize_withoutLockedResults[L\\x00\\x00\\x00\\t\\x00\\x00\\x00x\\x9cce``(-N-2\\xb8\\xc4\\xc2\\xc0\\xc0\\x08\\xe40\\n\\x02\\tg\\xab\\x98\\x94\\xcc\"\\x83\\x98\\xe2\\xfc\\xbct\\x03\\xbd\\xdc\\x02\\xe3\\x86i3\\x18@\\x80\\x19\\x88\\x81\\&\\x06\\x88b\\x07 \\x01b\\xa7\\x80\\xf8/\\x80\\x92\\xac\\x0c\\x10\\x00\\x00\\xbd\\x03\\r\\x03], test_PeerSearchReply_Request_serialize_withoutLockedResults")
        void peerSearchReply_Request_deserialize_withoutLockedResults_L_x00_x00_x00_t_x00_x00_x00x_x9cce____N_2_xb8_xc4_xc2_xc0_xc0_x08_xe40_n_x02_tg_xab_x98_x94_xcc__x83_x98_xe2_xfc_xbct_x03_xbd_xdc_x02_xe3_x86i3_x18__x80_x19_x88_x81___x06_x88b_x07__x01b_xa7_x80_xf8__x80_x92_xac_x0c_x10_x00_x00_xbd_x03_r_x03__decodes() {
            assertNotNull(assertDoesNotThrow(() -> SearchResponseFactory.fromByteArray(hex("4c00000009000000789c63656060282d4e2d32b8c4c2c0c008e4300a020967ab9894cc228398e2fcbc7403bddc02e38669331840801988815c2606886207200162a780f82f8092ac0c100000bd030d03"))));
        }

        @Test
        @DisplayName("test_PeerSearchReply_Request_deserialize_withoutLockedResults[W\\x00\\x00\\x00\\t\\x00\\x00\\x00x\\x9cce``(-N-2\\xb8\\xc4\\xc2\\xc0\\xc0\\xc8\\xc0\\xc0\\xc0(\\xc8\\xc0\\xc0\\xe0l\\x15\\x93\\x92Yd\\x10S\\x9c\\x9f\\x97n\\xa0\\x97[`\\xdc0m\\x06\\x03\\x080300\\xe4\\x16\\x183\\x81\\x142008020\\x80\\xd8) \\xfe\\x0bf\\x06\\x06V\\xb0*\\x06\\x06\\x00\\xbd\\x03\\r\\x03]")
        void peerSearchReply_Request_deserialize_withoutLockedResults_W_x00_x00_x00_t_x00_x00_x00x_x9cce____N_2_xb8_xc4_xc2_xc0_xc0_xc8_xc0_xc0_xc0__xc8_xc0_xc0_xe0l_x15_x93_x92Yd_x10S_x9c_x9f_x97n_xa0_x97___xdc0m_x06_x03_x080300_xe4_x16_x183_x81_x142008020_x80_xd8___xfe_x0bf_x06_x06V_xb0__x06_x06_x00_xbd_x03_r_x03__decodes() {
            assertNotNull(assertDoesNotThrow(() -> SearchResponseFactory.fromByteArray(hex("5700000009000000789c63656060282d4e2d32b8c4c2c0c0c8c0c0c028c8c0c0e06c159392596410539c9f976ea0975b60dc306d06030830333030e416183381143230303830323080d82920fe0b66060656b02a060600bd030d03"))));
        }

        @Test
        @DisplayName("test_PeerSearchReply_Request_deserialize_withLockedResults[[\\x00\\x00\\x00\\t\\x00\\x00\\x00x\\x9cce``(-N-2\\xb8\\xc4\\xc2\\xc0\\xc0\\x08\\xe40\\n\\x02\\tg\\xab\\x98\\x94\\xcc\"\\x83\\x98\\xe2\\xfc\\xbct\\x03\\xbd\\xdc\\x02\\xe3\\x86i3\\x18@\\x80\\x19\\x88\\x81\\&\\x06\\x88b\\x07 \\x01b\\xa7\\x80\\xf8/\\x80\\x92\\xac\\x0c\\x10\\x006I\\x02\\xc9\\xa4\\x9c\\xfc\\xe4\\xec\\xd4\\x94xR\\x0c\\x04\\x00&\\xfe\\x19\"], test_PeerSearchReply_Request_serialize_withLockedResults")
        void peerSearchReply_Request_deserialize_withLockedResults___x00_x00_x00_t_x00_x00_x00x_x9cce____N_2_xb8_xc4_xc2_xc0_xc0_x08_xe40_n_x02_tg_xab_x98_x94_xcc__x83_x98_xe2_xfc_xbct_x03_xbd_xdc_x02_xe3_x86i3_x18__x80_x19_x88_x81___x06_x88b_x07__x01b_xa7_x80_xf8__x80_x92_xac_x0c_x10_x006I_x02_xc9_xa4_x9c_xfc_xe4_xec_xd4_x94xR_x0c_x04_x00__xfe_x19___decodes() {
            assertNotNull(assertDoesNotThrow(() -> SearchResponseFactory.fromByteArray(hex("5b00000009000000789c63656060282d4e2d32b8c4c2c0c008e4300a020967ab9894cc228398e2fcbc7403bddc02e38669331840801988815c2606886207200162a780f82f8092ac0c1000364902c9a49cfce4ecd49478520c040026fe1922"))));
        }

        @Test
        @DisplayName("test_PeerSearchReply_Request_deserialize_withLockedResults[d\\x00\\x00\\x00\\t\\x00\\x00\\x00x\\x9cce``(-N-2\\xb8\\xc4\\xc2\\xc0\\xc0\\xc8\\xc0\\xc0\\xc0(\\xc8\\xc0\\xc0\\xe0l\\x15\\x93\\x92Yd\\x10S\\x9c\\x9f\\x97n\\xa0\\x97[`\\xdc0m\\x06\\x03\\x080300\\xe4\\x16\\x183\\x81\\x142008020\\x80\\xd8) \\xfe\\x0bf\\x06\\x06V\\xb0*\\xa8I\\x12H&\\xe5\\xe4'g\\xa7\\xa6\\xc4\\x93b \\x00&\\xfe\\x19\"]")
        void peerSearchReply_Request_deserialize_withLockedResults_d_x00_x00_x00_t_x00_x00_x00x_x9cce____N_2_xb8_xc4_xc2_xc0_xc0_xc8_xc0_xc0_xc0__xc8_xc0_xc0_xe0l_x15_x93_x92Yd_x10S_x9c_x9f_x97n_xa0_x97___xdc0m_x06_x03_x080300_xe4_x16_x183_x81_x142008020_x80_xd8___xfe_x0bf_x06_x06V_xb0__xa8I_x12H__xe5_xe4_g_xa7_xa6_xc4_x93b__x00__xfe_x19___decodes() {
            assertNotNull(assertDoesNotThrow(() -> SearchResponseFactory.fromByteArray(hex("6400000009000000789c63656060282d4e2d32b8c4c2c0c0c8c0c0c028c8c0c0e06c159392596410539c9f976ea0975b60dc306d06030830333030e416183381143230303830323080d82920fe0b66060656b02aa849124826e5e42767a7a6c49362200026fe1922"))));
        }
    }

    @Nested
    @DisplayName("PeerSharesReply")
    class PeerSharesReplyVectors {

        @Test
        @DisplayName("test_PeerSharesReply_Request_deserialize_withoutLockedResults[A\\x00\\x00\\x00\\x05\\x00\\x00\\x00x\\x9ccd```\\x07bg\\xab\\x98\\x94\\xcc\"\\x03F \\x93\\x81\\x13\\x88\\x8b\\xf3\\xf3\\xd2\\r\\xf4r\\x0b\\x8c\\x1d\\x9c\\xf8Ab\\x0c\\xcc@\\x0c\\xe42\\x01)\\x90\"\\x07 \\x01b\\xef\\xe0\\x06\\xcb2\\x00\\x00Wf\\x08-], test_PeerSharesReply_Request_serialize_withoutLockedResults")
        void peerSharesReply_Request_deserialize_withoutLockedResults_A_x00_x00_x00_x05_x00_x00_x00x_x9ccd____x07bg_xab_x98_x94_xcc__x03F__x93_x81_x13_x88_x8b_xf3_xf3_xd2_r_xf4r_x0b_x8c_x1d_x9c_xf8Ab_x0c_xcc__x0c_xe42_x01__x90__x07__x01b_xef_xe0_x06_xcb2_x00_x00Wf_x08___decodes() {
            assertNotNull(assertDoesNotThrow(() -> BrowseResponseFactory.fromByteArray(hex("4100000005000000789c6364606060076267ab9894cc22034620938113888bf3f3d20df4720b8c1d9cf841620ccc400ce4320129902207200162efe006cb3200005766082d"))));
        }

        @Test
        @DisplayName("test_PeerSharesReply_Request_deserialize_withoutLockedResults[J\\x00\\x00\\x00\\x05\\x00\\x00\\x00x\\x9ccd```g``p\\xb6\\x8aI\\xc9,2`d```\\xe0d``(\\xce\\xcfK7\\xd0\\xcb-0vp\\xe2\\x07\\x8910300\\xe4\\x16\\x183100\\x80\\x149020\\x80\\xd8;\\xb8\\xc1\\xb2\\x0c\\x00Wf\\x08-]")
        void peerSharesReply_Request_deserialize_withoutLockedResults_J_x00_x00_x00_x05_x00_x00_x00x_x9ccd___g__p_xb6_x8aI_xc9_2_d____xe0d____xce_xcfK7_xd0_xcb_0vp_xe2_x07_x8910300_xe4_x16_x183100_x80_x149020_x80_xd8__xb8_xc1_xb2_x0c_x00Wf_x08___decodes() {
            assertNotNull(assertDoesNotThrow(() -> BrowseResponseFactory.fromByteArray(hex("4a00000005000000789c636460606067606070b68a49c92c326064606060e064606028cecf4b37d0cb2d307670e207893130333030e416183331303080143930323080d83bb8c1b20c005766082d"))));
        }

        @Test
        @DisplayName("test_PeerSharesReply_Request_deserialize_withLockedResults[T\\x00\\x00\\x00\\x05\\x00\\x00\\x00x\\x9ccd```\\x07bg\\xab\\x98\\x94\\xcc\"\\x03F \\x93\\x91\\x13H\\x14\\xe7\\xe7\\xa5\\x1b\\xe8\\xe5\\x16\\x18;8\\xf13\\x80\\x003\\x10\\x03\\xb9L \\x05@\\xec\\x00$@\\xec\\x1d\\xdc`Y\\xb0\\x18\\x1f\\xc4\\x9c\\x9c\\xfc\\xe4\\xec\\xd4\\x94x\\xb8q\\x02@\\x02*F\\x8a\\xa9\\x00\\x8e\\xa3\\x16\\x0b], test_PeerSharesReply_Request_serialize_withLockedResults")
        void peerSharesReply_Request_deserialize_withLockedResults_T_x00_x00_x00_x05_x00_x00_x00x_x9ccd____x07bg_xab_x98_x94_xcc__x03F__x93_x91_x13H_x14_xe7_xe7_xa5_x1b_xe8_xe5_x16_x18_8_xf13_x80_x003_x10_x03_xb9L__x05__xec_x00___xec_x1d_xdc_Y_xb0_x18_x1f_xc4_x9c_x9c_xfc_xe4_xec_xd4_x94x_xb8q_x02__x02_F_x8a_xa9_x00_x8e_xa3_x16_x0b__decodes() {
            assertNotNull(assertDoesNotThrow(() -> BrowseResponseFactory.fromByteArray(hex("5400000005000000789c6364606060076267ab9894cc220346209391134814e7e7a51be8e516183b38f1338000331003b94c200540ec002440ec1ddc6059b0181fc49c9cfce4ecd49478b8710240022a468aa9008ea3160b"))));
        }

        @Test
        @DisplayName("test_PeerSharesReply_Request_deserialize_withLockedResults[_\\x00\\x00\\x00\\x05\\x00\\x00\\x00x\\x9ccd```g``p\\xb6\\x8aI\\xc9,2`d```\\xe4d``(\\xce\\xcfK7\\xd0\\xcb-0vp\\xe2g\\x00\\x01f\\x06\\x06\\x86\\xdc\\x02c&\\x90\\x02\\x06\\x06\\x06\\x07F\\x06\\x06\\x10{\\x077X\\x16,\\xc6\\x071''?9;5%\\x1en\\x9c\\x00\\x03\\x03\\x03T\\x8c\\x14S\\x01\\x8e\\xa3\\x16\\x0b]")
        void peerSharesReply_Request_deserialize_withLockedResults___x00_x00_x00_x05_x00_x00_x00x_x9ccd___g__p_xb6_x8aI_xc9_2_d____xe4d____xce_xcfK7_xd0_xcb_0vp_xe2g_x00_x01f_x06_x06_x86_xdc_x02c__x90_x02_x06_x06_x06_x07F_x06_x06_x10__x077X_x16__xc6_x071___9_5__x1en_x9c_x00_x03_x03_x03T_x8c_x14S_x01_x8e_xa3_x16_x0b__decodes() {
            assertNotNull(assertDoesNotThrow(() -> BrowseResponseFactory.fromByteArray(hex("5f00000005000000789c636460606067606070b68a49c92c326064606060e464606028cecf4b37d0cb2d307670e267000166060686dc026326900206060607460606107b073758162cc6073127273f393b35251e6e9c00030303548c1453018ea3160b"))));
        }
    }

    @Nested
    @DisplayName("PeerSharesRequest")
    class PeerSharesRequestVectors {

        @Test
        @DisplayName("test_PeerSharesRequest_Request_deserialize, test_PeerSharesRequest_Request_serialize")
        void peerSharesRequest_Request_deserialize() {
            assertArrayEquals(hex("0400000004000000"), new BrowseRequest().toByteArray());
        }
    }

    @Nested
    @DisplayName("PeerTransferQueue")
    class PeerTransferQueueVectors {

        @Test
        @DisplayName("test_PeerTransferQueue_Request_deserialize, test_PeerTransferQueue_Request_serialize")
        void peerTransferQueue_Request_deserialize() {
            assertArrayEquals(hex("190000002b00000011000000433a5c646972305c736f6e67302e6d7033"), new QueueDownloadRequest("C:\\dir0\\song0.mp3").toByteArray());
        }
    }

    @Nested
    @DisplayName("PeerTransferQueueFailed")
    class PeerTransferQueueFailedVectors {

        @Test
        @DisplayName("test_PeerTransferQueueFailed_Request_deserialize, test_PeerTransferQueueFailed_Request_serialize")
        void peerTransferQueueFailed_Request_deserialize() {
            assertArrayEquals(hex("260000003200000011000000433a5c646972305c736f6e67302e6d70330900000043616e63656c6c6564"), new UploadDenied("C:\\dir0\\song0.mp3", "Cancelled").toByteArray());
        }
    }

    @Nested
    @DisplayName("PeerTransferReply")
    class PeerTransferReplyVectors {

        @Test
        @DisplayName("test_PeerTransferReply_Request_deserialize_withFilesize, test_PeerTransferReply_Request_serialize_withFilesize")
        void peerTransferReply_Request_deserialize_withFilesize_decodes() {
            assertNotNull(assertDoesNotThrow(() -> TransferResponse.fromByteArray(hex("1100000029000000d20400000140420f0000000000"))));
        }

        @Test
        @DisplayName("test_PeerTransferReply_Request_deserialize_withReason, test_PeerTransferReply_Request_serialize_withReason")
        void peerTransferReply_Request_deserialize_withReason_decodes() {
            assertNotNull(assertDoesNotThrow(() -> TransferResponse.fromByteArray(hex("1600000029000000d2040000000900000043616e63656c6c6564"))));
        }
    }

    @Nested
    @DisplayName("PeerTransferRequest")
    class PeerTransferRequestVectors {

        @Test
        @DisplayName("test_PeerTransferRequest_Request_deserialize_withFilesize, test_PeerTransferRequest_Request_serialize_withFilesize")
        void peerTransferRequest_Request_deserialize_withFilesize() {
            assertArrayEquals(hex("290000002800000001000000d204000011000000433a5c646972305c736f6e67302e6d7033a086010000000000"), new TransferRequest(TransferDirection.UPLOAD, 1234, "C:\\dir0\\song0.mp3", 100000).toByteArray());
        }
    }

    @Nested
    @DisplayName("PeerUploadFailed")
    class PeerUploadFailedVectors {

        @Test
        @DisplayName("test_PeerUploadFailed_Request_deserialize, test_PeerUploadFailed_Request_serialize")
        void peerUploadFailed_Request_deserialize() {
            assertArrayEquals(hex("190000002e00000011000000433a5c646972305c736f6e67302e6d7033"), new UploadFailed("C:\\dir0\\song0.mp3").toByteArray());
        }
    }

    @Nested
    @DisplayName("PeerUserInfoReply")
    class PeerUserInfoReplyVectors {

        @Test
        @DisplayName("test_PeerUserInfoReply_Request_deserialize_withoutPicture, test_PeerUserInfoReply_Request_serialize_withoutPicture")
        void peerUserInfoReply_Request_deserialize_withoutPicture_decodes() {
            assertNotNull(assertDoesNotThrow(() -> UserInfoResponseFactory.fromByteArray(hex("1d000000100000000b0000006465736372697074696f6e00050000000a00000001"))));
        }

        @Test
        @DisplayName("test_PeerUserInfoReply_Request_deserialize_withoutPictureWithPermissions, test_PeerUserInfoReply_Request_serialize_withoutPictureWithPermissions")
        void peerUserInfoReply_Request_deserialize_withoutPictureWithPermissions_decodes() {
            assertNotNull(assertDoesNotThrow(() -> UserInfoResponseFactory.fromByteArray(hex("21000000100000000b0000006465736372697074696f6e00050000000a0000000100000000"))));
        }

        @Test
        @DisplayName("test_PeerUserInfoReply_Request_deserialize_withPicture, test_PeerUserInfoReply_Request_serialize_withPicture")
        void peerUserInfoReply_Request_deserialize_withPicture_decodes() {
            assertNotNull(assertDoesNotThrow(() -> UserInfoResponseFactory.fromByteArray(hex("26000000100000000b0000006465736372697074696f6e0105000000aabbccddee050000000a00000001"))));
        }

        @Test
        @DisplayName("test_PeerUserInfoReply_Request_deserialize_withPictureWithPermissions, test_PeerUserInfoReply_Request_serialize_withPictureWithPermissions")
        void peerUserInfoReply_Request_deserialize_withPictureWithPermissions_decodes() {
            assertNotNull(assertDoesNotThrow(() -> UserInfoResponseFactory.fromByteArray(hex("2a000000100000000b0000006465736372697074696f6e0105000000aabbccddee050000000a0000000103000000"))));
        }
    }

    @Nested
    @DisplayName("PeerUserInfoRequest")
    class PeerUserInfoRequestVectors {

        @Test
        @DisplayName("test_PeerUserInfoRequest_Request_deserialize, test_PeerUserInfoRequest_Request_serialize, test_whenDeserializePeerRequest_shouldDeserialize")
        void peerUserInfoRequest_Request_deserialize() {
            assertArrayEquals(hex("040000000f000000"), new UserInfoRequest().toByteArray());
        }
    }
}
