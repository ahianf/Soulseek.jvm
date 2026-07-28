// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The guard on losing capability.
 *
 * <p>{@code capability-baseline.txt} is what the library could do before the
 * rewrite, captured from the old client by reflection before anything was
 * deleted. {@code capability-dispositions.txt} says where each of those went.
 * This test holds the two together: every baseline member needs a disposition,
 * and a disposition naming a facet member has to name one that exists.
 *
 * <p>It is deliberately not satisfiable by deleting things. A capability with no
 * replacement is a design gap to raise, not a deletion to make, and the only way
 * to make this test pass for a member is to point at the thing that now does its
 * job — or to state, in writing, that the library absorbed it, that it moved to
 * construction, or which later phase brings it back.
 */
class CapabilityExhaustivenessTest {

    private static final Path BASELINE = Path.of("src", "test", "resources", "capability-baseline.txt");
    private static final Path DISPOSITIONS = Path.of("src", "test", "resources", "capability-dispositions.txt");

    /** Strips modifiers, generics and the argument list down to a bare member name. */
    private static final Pattern MEMBER =
            Pattern.compile("^public (?:abstract |static |final )*[^ ]+ ([A-Za-z][A-Za-z0-9_]*)\\(");

    private static final Set<String> FACETS = Set.of(
            "Soulseek",
            "Connection",
            "Search",
            "Downloads",
            "Uploads",
            "Users",
            "Rooms",
            "PrivateRooms",
            "Chat",
            "Shares",
            "Me",
            "Diagnostics",
            "Watch");

    private static Set<String> baselineMembers() throws IOException {
        Set<String> names = new TreeSet<>();
        for (String line : Files.readAllLines(BASELINE)) {
            if (line.startsWith("#") || line.isBlank()) {
                continue;
            }
            Matcher matcher = MEMBER.matcher(line);
            assertTrue(matcher.find(), "could not parse a member name from: " + line);
            names.add(matcher.group(1));
        }
        return names;
    }

    private static Map<String, String> dispositions() throws IOException {
        Map<String, String> rows = new LinkedHashMap<>();
        for (String line : Files.readAllLines(DISPOSITIONS)) {
            if (line.startsWith("#") || line.isBlank()) {
                continue;
            }
            int equals = line.indexOf('=');
            assertTrue(equals > 0, "not a disposition row: " + line);
            rows.put(
                    line.substring(0, equals).trim(), line.substring(equals + 1).trim());
        }
        return rows;
    }

    @Test
    @DisplayName("every capability of the old client has a disposition")
    void everyBaselineMemberIsAccountedFor() throws IOException {
        Set<String> baseline = baselineMembers();
        Map<String, String> dispositions = dispositions();

        List<String> undisposed = new ArrayList<>();
        for (String member : baseline) {
            // The remove half of a listener pair shares its add's row: the
            // stream replaces registration and Subscription.close replaces
            // removal, so there is one decision, not two.
            String key = member.startsWith("remove") && member.endsWith("Listener")
                    ? "add" + member.substring("remove".length())
                    : member;
            if (!dispositions.containsKey(key)) {
                undisposed.add(member);
            }
        }

        assertTrue(
                undisposed.isEmpty(),
                "these capabilities have no disposition, so deleting the old client would lose them:"
                        + System.lineSeparator()
                        + String.join(System.lineSeparator(), undisposed));
    }

    @Test
    @DisplayName("a disposition naming a facet member names one that exists")
    void everyNamedReplacementResolves() throws IOException {
        List<String> unresolved = new ArrayList<>();

        for (Map.Entry<String, String> row : dispositions().entrySet()) {
            String disposition = row.getValue();
            if (disposition.startsWith("ABSORBED:")
                    || disposition.startsWith("BUILDER:")
                    || disposition.startsWith("PENDING:")) {
                assertTrue(
                        disposition.length() > disposition.indexOf(':') + 1,
                        row.getKey() + ": a non-facet disposition must say why");
                continue;
            }

            int dot = disposition.lastIndexOf('.');
            assertTrue(dot > 0, row.getKey() + ": malformed disposition '" + disposition + "'");
            String typeName = disposition.substring(0, dot);
            String memberName = disposition.substring(dot + 1);

            assertTrue(FACETS.contains(typeName), row.getKey() + ": '" + typeName + "' is not a facet");

            try {
                Class<?> type = Class.forName("dev.slsk." + typeName);
                boolean found =
                        Arrays.stream(type.getMethods()).map(Method::getName).anyMatch(memberName::equals);
                if (!found) {
                    unresolved.add(row.getKey() + " -> " + disposition);
                }
            } catch (ClassNotFoundException exception) {
                unresolved.add(row.getKey() + " -> " + disposition + " (no such type)");
            }
        }

        assertTrue(
                unresolved.isEmpty(),
                "these dispositions name a facet member that does not exist:"
                        + System.lineSeparator()
                        + String.join(System.lineSeparator(), unresolved));
    }

    @Test
    @DisplayName("no disposition row is stale")
    void everyDispositionRowMatchesABaselineMember() throws IOException {
        Set<String> baseline = baselineMembers();
        List<String> stale = new ArrayList<>();
        for (String key : dispositions().keySet()) {
            boolean matches = baseline.contains(key)
                    || (key.startsWith("add")
                            && key.endsWith("Listener")
                            && baseline.contains("remove" + key.substring("add".length())));
            if (!matches) {
                stale.add(key);
            }
        }
        assertTrue(
                stale.isEmpty(),
                "these rows name nothing in the baseline:" + System.lineSeparator()
                        + String.join(System.lineSeparator(), stale));
    }

    @Test
    @DisplayName("the baseline still describes 202 members, and has not been quietly trimmed")
    void baselineIsIntact() throws IOException {
        assertEquals(
                202,
                Files.readAllLines(BASELINE).stream()
                        .filter(line -> !line.startsWith("#") && !line.isBlank())
                        .count());
    }
}
