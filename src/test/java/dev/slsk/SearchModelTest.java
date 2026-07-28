// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SearchModelTest {

    private static SearchFile file(String path, long size, int bitrate) {
        return new SearchFile(path, size, new FileAttributes(Map.of(FileAttributeType.BIT_RATE, bitrate)));
    }

    @Nested
    class FileTest {

        @Test
        @DisplayName("names and extensions come off the backslash-joined remote path")
        void derivesNameAndExtension() {
            SearchFile file = file("@@music\\Album\\Track.MP3", 100, 320);
            assertEquals("Track.MP3", file.name());
            assertEquals("mp3", file.extension(), "lowercased, for matching");
        }

        @Test
        void handlesAFileWithNoExtension() {
            assertEquals("", file("share\\README", 1, 0).extension());
            assertEquals("", file("share\\trailing.", 1, 0).extension());
        }

        @Test
        void exposesAttributesByName() {
            FileAttributes attributes = new FileAttributes(Map.of(
                    FileAttributeType.BIT_RATE, 320,
                    FileAttributeType.LENGTH, 245,
                    FileAttributeType.VARIABLE_BIT_RATE, 1,
                    FileAttributeType.SAMPLE_RATE, 44100,
                    FileAttributeType.BIT_DEPTH, 16));
            assertEquals(320, attributes.bitrate().getAsInt());
            assertEquals(Duration.ofSeconds(245), attributes.duration().orElseThrow());
            assertTrue(attributes.variableBitRate());
            assertEquals(44100, attributes.sampleRate().getAsInt());
            assertEquals(16, attributes.bitDepth().getAsInt());
        }

        @Test
        @DisplayName("an attribute the peer did not send is absent, not zero")
        void missingAttributesAreAbsent() {
            FileAttributes none = FileAttributes.none();
            assertTrue(none.bitrate().isEmpty());
            assertTrue(none.duration().isEmpty());
            assertTrue(none.sampleRate().isEmpty());
            assertFalse(none.variableBitRate());
        }

        @Test
        @DisplayName("the raw map is kept, so an attribute this library does not model survives")
        void rawMapIsPreserved() {
            FileAttributes attributes = new FileAttributes(Map.of(FileAttributeType.BIT_DEPTH, 24));
            assertEquals(Map.of(FileAttributeType.BIT_DEPTH, 24), attributes.raw());
            assertThrows(
                    UnsupportedOperationException.class, () -> attributes.raw().put(FileAttributeType.BIT_RATE, 1));
        }

        @Test
        void attributeTypeCodesAreNotContiguous() {
            assertEquals(2, FileAttributeType.VARIABLE_BIT_RATE.code());
            assertEquals(4, FileAttributeType.SAMPLE_RATE.code());
            assertEquals(FileAttributeType.SAMPLE_RATE, FileAttributeType.fromCode(4));
            assertEquals(null, FileAttributeType.fromCode(3), "there is no attribute 3");
        }
    }

    @Nested
    class FiltersTest {

        @Test
        void noneKeepsEverything() {
            assertTrue(SearchFilters.none().accepts(file("a\\b.mp3", 1, 0), false));
            assertTrue(SearchFilters.none().accepts(file("a\\b.mp3", 1, 0), true), "locked too");
        }

        @Test
        void filtersBySize() {
            SearchFilters filters =
                    new SearchFilters(OptionalInt.empty(), OptionalLong.of(100), OptionalLong.of(200), false, Set.of());
            assertFalse(filters.accepts(file("a\\b.mp3", 99, 0), false));
            assertTrue(filters.accepts(file("a\\b.mp3", 100, 0), false));
            assertTrue(filters.accepts(file("a\\b.mp3", 200, 0), false));
            assertFalse(filters.accepts(file("a\\b.mp3", 201, 0), false));
        }

        @Test
        @DisplayName("a minimum bitrate drops files whose bitrate the peer never stated")
        void unknownBitrateFailsAMinimum() {
            SearchFilters filters =
                    new SearchFilters(OptionalInt.of(256), OptionalLong.empty(), OptionalLong.empty(), false, Set.of());
            assertTrue(filters.accepts(file("a\\b.mp3", 1, 320), false));
            assertFalse(filters.accepts(file("a\\b.mp3", 1, 128), false));
            assertFalse(
                    filters.accepts(new SearchFile("a\\b.mp3", 1, FileAttributes.none()), false),
                    "cannot prove it meets the minimum");
        }

        @Test
        void filtersByExtension() {
            SearchFilters filters = new SearchFilters(
                    OptionalInt.empty(), OptionalLong.empty(), OptionalLong.empty(), false, Set.of("flac", "mp3"));
            assertTrue(filters.accepts(file("a\\b.MP3", 1, 0), false), "matching is case-insensitive");
            assertTrue(filters.accepts(file("a\\b.flac", 1, 0), false));
            assertFalse(filters.accepts(file("a\\b.wav", 1, 0), false));
        }

        @Test
        void excludesLockedFilesOnRequest() {
            SearchFilters filters =
                    new SearchFilters(OptionalInt.empty(), OptionalLong.empty(), OptionalLong.empty(), true, Set.of());
            assertTrue(filters.accepts(file("a\\b.mp3", 1, 0), false));
            assertFalse(filters.accepts(file("a\\b.mp3", 1, 0), true));
        }
    }

    @Nested
    class QueryTest {

        @Test
        void defaultsToANetworkSearch() {
            SearchQuery query = SearchQuery.of("aphex twin");
            assertEquals(SearchScope.Kind.NETWORK, query.scope().kind());
            assertEquals(SearchLimits.defaults(), query.limits());
            assertEquals(SearchFilters.none(), query.filters());
        }

        @Test
        @DisplayName("the idle timeout is shorter than the overall one, which is the point of it")
        void defaultLimitsStopOnSilence() {
            assertEquals(Duration.ofSeconds(15), SearchLimits.defaults().overall());
            assertEquals(Duration.ofSeconds(4), SearchLimits.defaults().idle());
            assertTrue(SearchLimits.defaults()
                            .idle()
                            .compareTo(SearchLimits.defaults().overall())
                    < 0);
        }

        @Test
        void rejectsBlankTerms() {
            assertThrows(IllegalArgumentException.class, () -> SearchQuery.of(" "));
            assertThrows(NullPointerException.class, () -> SearchQuery.of(null));
        }

        @Test
        void rejectsNonsenseLimits() {
            assertThrows(
                    IllegalArgumentException.class, () -> new SearchLimits(Duration.ZERO, Duration.ofSeconds(1), 1, 1));
            assertThrows(
                    IllegalArgumentException.class, () -> new SearchLimits(Duration.ofSeconds(1), Duration.ZERO, 1, 1));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new SearchLimits(Duration.ofSeconds(1), Duration.ofSeconds(1), 0, 1));
        }

        @Test
        @DisplayName("a room search names exactly one room, because the wire carries one")
        void roomScopeTakesOneRoom() {
            assertEquals(SearchScope.Kind.ROOM, SearchScope.room("lobby").kind());
            assertEquals("lobby", SearchScope.room("lobby").targets().get(0));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new SearchScope(SearchScope.Kind.ROOM, java.util.List.of("a", "b")));
            assertThrows(
                    IllegalArgumentException.class, () -> new SearchScope(SearchScope.Kind.ROOM, java.util.List.of()));
        }

        @Test
        void userScopeTakesSeveral() {
            SearchScope scope = SearchScope.users(Username.of("bob"), Username.of("carol"));
            assertEquals(SearchScope.Kind.USER, scope.kind());
            assertEquals(java.util.List.of("bob", "carol"), scope.targets());
        }

        @Test
        void withersReplaceOnePartAtATime() {
            SearchQuery query = SearchQuery.of("x").withScope(SearchScope.room("lobby"));
            assertEquals(SearchScope.Kind.ROOM, query.scope().kind());
            assertEquals("x", query.terms());
        }
    }

    @Nested
    class StatusTest {

        @Test
        @DisplayName("every terminal status is a policy this library invented, not a message")
        void onlyInProgressIsNonTerminal() {
            assertFalse(SearchStatus.IN_PROGRESS.isTerminal());
            assertTrue(SearchStatus.COMPLETED.isTerminal());
            assertTrue(SearchStatus.CANCELLED.isTerminal());
            assertTrue(SearchStatus.TIMED_OUT.isTerminal());
        }
    }
}
