// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationSignal;
import dev.slsk.Connection;
import dev.slsk.ShareIndex;
import dev.slsk.SharedFolder;
import dev.slsk.Soulseek;
import dev.slsk.internal.options.SoulseekClientOptions;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SoulseekShapeTest {

    private static Soulseek client() {
        return DefaultSoulseek.create("alice", "password", 157, new SoulseekClientOptions());
    }

    @Test
    @DisplayName("the root type is exactly ten facet accessors and close()")
    void rootTypeIsTenFacetsAndClose() {
        // Instance members only: builder() is static, and the constraint is
        // about what the root type carries, not how you get one.
        Set<String> methods = Arrays.stream(Soulseek.class.getMethods())
                .filter(method -> !java.lang.reflect.Modifier.isStatic(method.getModifiers()))
                .map(Method::getName)
                .collect(Collectors.toSet());

        assertEquals(
                Set.of(
                        "connection",
                        "search",
                        "downloads",
                        "uploads",
                        "users",
                        "rooms",
                        "chat",
                        "shares",
                        "me",
                        "diagnostics",
                        "close"),
                methods,
                "the root type carries nothing beyond the ten facets and close()");
    }

    @Test
    @DisplayName("no facet declares more than twenty methods")
    void facetsStaySmall() {
        for (Class<?> facet : List.of(
                dev.slsk.Connection.class,
                dev.slsk.Search.class,
                dev.slsk.Downloads.class,
                dev.slsk.Uploads.class,
                dev.slsk.Users.class,
                dev.slsk.Rooms.class,
                dev.slsk.Chat.class,
                dev.slsk.Shares.class,
                dev.slsk.Me.class,
                dev.slsk.Diagnostics.class)) {
            int declared = facet.getDeclaredMethods().length;
            assertTrue(declared <= 20, facet.getSimpleName() + " declares " + declared + " methods");
        }
    }

    @Test
    @DisplayName("no exported signature mentions a username as a bare String")
    void noBareStringUsernamesOnFacets() {
        // The whole point of the Username type. Room names are still Strings,
        // which is why this checks parameter types rather than counting.
        for (Method method : dev.slsk.Chat.class.getDeclaredMethods()) {
            if (method.getName().equals("send")) {
                assertEquals(dev.slsk.user.Username.class, method.getParameterTypes()[0]);
            }
        }
        // Not "the first parameter is a Username": browse takes a request
        // object, which carries one. What must never happen is a bare String
        // standing in for a user, which is what the compiler cannot protect.
        for (Method method : dev.slsk.Users.class.getDeclaredMethods()) {
            if (method.getParameterCount() == 0) {
                continue;
            }
            Class<?> first = method.getParameterTypes()[0];
            assertTrue(
                    first == dev.slsk.user.Username.class || first == dev.slsk.user.BrowseRequest.class,
                    method.getName() + " leads with " + first.getSimpleName() + ", not a user");
        }
    }

    @Test
    void everyFacetIsReachableAndDistinct() {
        try (Soulseek slsk = client()) {
            List<Object> facets = List.of(
                    slsk.connection(),
                    slsk.search(),
                    slsk.downloads(),
                    slsk.uploads(),
                    slsk.users(),
                    slsk.rooms(),
                    slsk.chat(),
                    slsk.shares(),
                    slsk.me(),
                    slsk.diagnostics());
            assertEquals(10, facets.size());
            assertEquals(10, facets.stream().distinct().count(), "each facet is its own object");
        }
    }

    @Test
    @DisplayName("a facet accessor returns the same instance every time")
    void facetsAreStable() {
        try (Soulseek slsk = client()) {
            Connection first = slsk.connection();
            assertTrue(first == slsk.connection());
        }
    }

    @Test
    void sharesStartEmptyAndAcceptConfiguration(@TempDir Path directory) throws Exception {
        try (Soulseek slsk = client()) {
            assertEquals(
                    ShareIndex.ScanStatus.NEVER_SCANNED, slsk.shares().index().status());
            assertEquals(List.of(), slsk.shares().configured());

            slsk.shares().configure(List.of(SharedFolder.of(directory)));
            assertEquals(1, slsk.shares().configured().size());
        }
    }

    @Test
    @DisplayName("a scan counts what is there and leaves the index ready")
    void scanCountsFiles(@TempDir Path directory) throws Exception {
        Files.writeString(directory.resolve("one.mp3"), "aaaa");
        Files.writeString(directory.resolve("two.mp3"), "bb");
        Files.createDirectory(directory.resolve("nested"));
        Files.writeString(directory.resolve("nested").resolve("three.mp3"), "c");

        try (Soulseek slsk = client()) {
            slsk.shares().configure(List.of(SharedFolder.of(directory)));
            // The offline announcement at the end of the scan is tolerated —
            // scanning before connect() is the natural order — so the scan
            // returns its index rather than throwing it away.
            slsk.shares().rescan(CancellationSignal.none());
            assertEquals(3, slsk.shares().index().fileCount());
            assertEquals(7, slsk.shares().index().totalBytes());
        }
    }

    @Test
    void configuredFoldersAreImmutable() {
        try (Soulseek slsk = client()) {
            List<SharedFolder> folders = slsk.shares().configured();
            assertThrows(UnsupportedOperationException.class, () -> folders.add(SharedFolder.of(Path.of("/tmp"))));
        }
    }
}
