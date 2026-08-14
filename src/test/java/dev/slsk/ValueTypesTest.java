// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.user.Username;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ValueTypesTest {

    @Nested
    class UsernameTest {

        @Test
        void acceptsAnOrdinaryName() {
            assertEquals("alice", Username.of("alice").value());
        }

        @Test
        @DisplayName("keeps case, because every correlation map is keyed on the exact string")
        void preservesCase() {
            assertEquals("AliCe", Username.of("AliCe").value());
            assertNotEquals(Username.of("alice"), Username.of("AliCe"));
        }

        @Test
        void rejectsNull() {
            assertThrows(NullPointerException.class, () -> Username.of(null));
        }

        @ParameterizedTest
        @ValueSource(strings = {"", " ", "\t", "   "})
        void rejectsBlank(String value) {
            assertThrows(IllegalArgumentException.class, () -> Username.of(value));
        }

        @ParameterizedTest
        @DisplayName("rejects control characters, which would corrupt message framing")
        // Octal escapes rather than the bytes themselves: a literal NUL in the
        // source makes git call the whole file binary. \037 is unit separator
        // and \177 is delete.
        @ValueSource(strings = {"a\0b", "a\nb", "a\rb", "a\037b", "a\177b"})
        void rejectsControlCharacters(String value) {
            assertThrows(IllegalArgumentException.class, () -> Username.of(value));
        }

        @Test
        @DisplayName("accepts the punctuation and non-ASCII the server actually allows")
        void acceptsUnusualButLegalNames() {
            for (String value : List.of("a b", "user.name", "user-name", "ünïcødé", "日本語", "1")) {
                assertEquals(value, Username.of(value).value());
            }
        }

        @Test
        void ordersByUnderlyingString() {
            assertTrue(Username.of("alice").compareTo(Username.of("bob")) < 0);
            assertEquals(0, Username.of("alice").compareTo(Username.of("alice")));
        }

        @Test
        void rendersAsTheNameItself() {
            assertEquals("alice", Username.of("alice").toString());
        }
    }

    @Nested
    class IdentifierTest {

        @Test
        void searchIdWrapsAndUnwraps() {
            assertEquals("abc", SearchId.of("abc").value());
            assertEquals("abc", SearchId.of("abc").toString());
        }

        @Test
        void searchIdFromTokenUsesTheTokenItself() {
            assertEquals("42", SearchId.ofToken(42).value());
            assertEquals(SearchId.of("42"), SearchId.ofToken(42));
        }

        @Test
        void transferIdWrapsAndUnwraps() {
            assertEquals("abc", TransferId.of("abc").value());
            assertEquals("abc", TransferId.of("abc").toString());
        }

        @Test
        void identifiersRejectNullAndBlank() {
            assertThrows(NullPointerException.class, () -> SearchId.of(null));
            assertThrows(IllegalArgumentException.class, () -> SearchId.of(" "));
            assertThrows(NullPointerException.class, () -> TransferId.of(null));
            assertThrows(IllegalArgumentException.class, () -> TransferId.of(" "));
        }

        @Test
        @DisplayName("the two id types do not compare equal even with the same value")
        void identifiersAreDistinctTypes() {
            assertNotEquals((Object) SearchId.of("1"), (Object) TransferId.of("1"));
        }
    }

    @Nested
    class BandwidthTest {

        @Test
        void zeroMeansUnlimited() {
            assertTrue(Bandwidth.unlimited().isUnlimited());
            assertEquals(0, Bandwidth.unlimited().bytesPerSecond());
            assertTrue(Bandwidth.ofBytesPerSecond(0).isUnlimited());
            assertEquals(Bandwidth.unlimited(), Bandwidth.ofBytesPerSecond(0));
        }

        @Test
        void aRealLimitIsNotUnlimited() {
            Bandwidth bandwidth = Bandwidth.ofBytesPerSecond(1024);
            assertFalse(bandwidth.isUnlimited());
            assertEquals(1024, bandwidth.bytesPerSecond());
        }

        @Test
        void convertsKibibytes() {
            assertEquals(1024, Bandwidth.ofKibibytesPerSecond(1).bytesPerSecond());
            assertEquals(1_048_576, Bandwidth.ofKibibytesPerSecond(1024).bytesPerSecond());
        }

        @Test
        @DisplayName("8 Mbit/s is 1 MB/s, decimal megabits as connections are sold")
        void convertsMegabits() {
            assertEquals(1_000_000, Bandwidth.ofMegabitsPerSecond(8).bytesPerSecond());
            assertEquals(12_500, Bandwidth.ofMegabitsPerSecond(0.1).bytesPerSecond());
        }

        @Test
        void rejectsNegative() {
            assertThrows(IllegalArgumentException.class, () -> Bandwidth.ofBytesPerSecond(-1));
            assertThrows(IllegalArgumentException.class, () -> new Bandwidth(-1));
        }

        @Test
        void rendersUnlimitedByName() {
            assertEquals("unlimited", Bandwidth.unlimited().toString());
            assertEquals("1024 B/s", Bandwidth.ofBytesPerSecond(1024).toString());
        }
    }

    @Nested
    class PriorityTest {

        @Test
        @DisplayName("declaration order runs lowest-priority first, so natural order is usable")
        void ordersLowToHigh() {
            assertEquals(List.of(Priority.LOW, Priority.NORMAL, Priority.HIGH), List.of(Priority.values()));
            assertTrue(Priority.HIGH.compareTo(Priority.NORMAL) > 0);
            assertTrue(Priority.LOW.compareTo(Priority.NORMAL) < 0);
        }
    }
}
