// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ExceptionHierarchyTest {
    @ParameterizedTest(name = "{0}")
    @MethodSource("standardExceptions")
    @DisplayName("Standard exceptions preserve all public constructors")
    void standardExceptionsPreserveConstructors(Class<? extends SoulseekClientException> exceptionType)
            throws ReflectiveOperationException {
        Constructor<? extends SoulseekClientException> empty = exceptionType.getConstructor();
        Constructor<? extends SoulseekClientException> withMessage = exceptionType.getConstructor(String.class);
        Constructor<? extends SoulseekClientException> withCause =
                exceptionType.getConstructor(String.class, Throwable.class);
        IllegalStateException cause = new IllegalStateException("cause");

        SoulseekClientException emptyException = empty.newInstance();
        SoulseekClientException messageException = withMessage.newInstance("message");
        SoulseekClientException causeException = withCause.newInstance("message", cause);

        assertNull(emptyException.getMessage());
        assertEquals("message", messageException.getMessage());
        assertEquals("message", causeException.getMessage());
        assertSame(cause, causeException.getCause());
        assertInstanceOf(RuntimeException.class, emptyException);
    }

    @ParameterizedTest(name = "{0} extends {1}")
    @MethodSource("inheritanceRelationships")
    @DisplayName("Exceptions preserve the C# inheritance tree")
    void exceptionsPreserveInheritance(
            Class<? extends SoulseekClientException> child, Class<? extends SoulseekClientException> parent) {
        assertTrue(parent.isAssignableFrom(child));
    }

    @Test
    @DisplayName("Transfer size mismatch preserves sizes, message, and cause")
    void transferSizeMismatchPreservesState() {
        IllegalArgumentException cause = new IllegalArgumentException("cause");

        TransferSizeMismatchException empty = new TransferSizeMismatchException(123L, 456L);
        TransferSizeMismatchException message = new TransferSizeMismatchException("mismatch", 123L, 456L);
        TransferSizeMismatchException withCause = new TransferSizeMismatchException("mismatch", 123L, 456L, cause);

        assertNull(empty.getMessage());
        assertEquals(123L, empty.getLocalSize());
        assertEquals(456L, empty.getRemoteSize());
        assertEquals("mismatch", message.getMessage());
        assertEquals(123L, message.getLocalSize());
        assertEquals(456L, message.getRemoteSize());
        assertSame(cause, withCause.getCause());
    }

    private static Stream<Class<? extends SoulseekClientException>> standardExceptions() {
        return Stream.of(
                AddressException.class,
                ConnectionException.class,
                ConnectionReadException.class,
                ConnectionWriteDroppedException.class,
                ConnectionWriteException.class,
                DownloadEnqueueException.class,
                DuplicateTokenException.class,
                DuplicateTransferException.class,
                KickedFromServerException.class,
                ListenException.class,
                LoginRejectedException.class,
                MessageCompressionException.class,
                MessageException.class,
                MessageReadException.class,
                NoResponseException.class,
                ProxyException.class,
                RoomException.class,
                RoomJoinForbiddenException.class,
                SoulseekClientException.class,
                TransferException.class,
                TransferNotFoundException.class,
                TransferRejectedException.class,
                TransferReportedFailedException.class,
                TransferStreamException.class,
                UserEndpointCacheException.class,
                UserEndpointException.class,
                UserNotFoundException.class,
                UserOfflineException.class);
    }

    private static Stream<Arguments> inheritanceRelationships() {
        return Stream.of(
                Arguments.of(AddressException.class, SoulseekClientException.class),
                Arguments.of(ConnectionException.class, SoulseekClientException.class),
                Arguments.of(ConnectionReadException.class, ConnectionException.class),
                Arguments.of(ConnectionWriteDroppedException.class, ConnectionException.class),
                Arguments.of(ConnectionWriteException.class, ConnectionException.class),
                Arguments.of(DownloadEnqueueException.class, SoulseekClientException.class),
                Arguments.of(DuplicateTokenException.class, SoulseekClientException.class),
                Arguments.of(DuplicateTransferException.class, TransferException.class),
                Arguments.of(KickedFromServerException.class, SoulseekClientException.class),
                Arguments.of(ListenException.class, SoulseekClientException.class),
                Arguments.of(LoginRejectedException.class, SoulseekClientException.class),
                Arguments.of(MessageCompressionException.class, MessageException.class),
                Arguments.of(MessageException.class, SoulseekClientException.class),
                Arguments.of(MessageReadException.class, MessageException.class),
                Arguments.of(NoResponseException.class, SoulseekClientException.class),
                Arguments.of(ProxyException.class, ConnectionException.class),
                Arguments.of(RoomException.class, SoulseekClientException.class),
                Arguments.of(RoomJoinForbiddenException.class, RoomException.class),
                Arguments.of(TransferException.class, SoulseekClientException.class),
                Arguments.of(TransferNotFoundException.class, TransferException.class),
                Arguments.of(TransferRejectedException.class, TransferException.class),
                Arguments.of(TransferReportedFailedException.class, SoulseekClientException.class),
                Arguments.of(TransferSizeMismatchException.class, SoulseekClientException.class),
                Arguments.of(TransferStreamException.class, SoulseekClientException.class),
                Arguments.of(UserEndpointCacheException.class, UserEndpointException.class),
                Arguments.of(UserEndpointException.class, SoulseekClientException.class),
                Arguments.of(UserNotFoundException.class, SoulseekClientException.class),
                Arguments.of(UserOfflineException.class, SoulseekClientException.class));
    }
}
