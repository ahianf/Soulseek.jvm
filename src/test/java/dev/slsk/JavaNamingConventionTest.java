// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class JavaNamingConventionTest {
    private static final String SELF = "JavaNamingConventionTest.java";
    private static final Pattern I_PREFIX_INTERFACE = Pattern.compile("\\binterface\\s+I[A-Z][A-Za-z0-9_]*\\b");
    private static final List<Pattern> STALE_JAVA_NAMES = List.of(
            Pattern.compile("Event" + "Args|event" + "args"),
            Pattern.compile("End" + "Point|IP" + "Address"),
            Pattern.compile("\\bis(?:Has|Can|Have)[A-Z][A-Za-z0-9_]*\\b"),
            Pattern.compile("\\bCancellation(?:TokenSource|Token|Registration)\\b"),
            Pattern.compile("\\b(?:SoulseekClientStates|SearchStates|TransferStates)\\b"),
            Pattern.compile("\\b(?:ISearchResponseCache|ISoulseekClient"
                    + "|IUserEndPointCache|IWaiter|IDiagnosticFactory"
                    + "|IDiagnosticGenerator|IDistributedMessageHandler"
                    + "|IMessageHandler|IPeerMessageHandler"
                    + "|IServerMessageHandler|IIncomingMessage"
                    + "|IInitializationMessage|IOutgoingMessage"
                    + "|IConnectionFactory|IDistributedConnectionManager"
                    + "|IListenerHandler|IMessageConnection"
                    + "|IPeerConnectionManager|IConnection|IListener"
                    + "|INetworkStream|ITcpClient|ITcpListener"
                    + "|ISearchResponder)\\b"),
            Pattern.compile("\\bDiagnostic" + "Factory\\b"),
            Pattern.compile("\\b(?:addOrUpdate|tryGet|tryRemove)\\s*\\("),
            Pattern.compile("\\bnew\\s+SoulseekClient\\s*\\("));

    @Test
    void productionDeclarationsAndIdentifiersUseJavaFirstNames() throws IOException {
        List<String> failures = new ArrayList<>();
        inspectTree(Path.of("src", "main", "java"), true, failures);
        assertNoFailures(failures);
    }

    @Test
    void testsUseOnlyTheSupportedJavaNames() throws IOException {
        List<String> failures = new ArrayList<>();
        for (Path root : List.of(Path.of("src", "test", "java"), Path.of("src", "integrationTest", "java"))) {
            inspectTree(root, false, failures);
        }
        assertNoFailures(failures);
    }

    /**
     * The ported documentation this used to walk was deleted with the C#-parity paper
     * trail, so only the surviving documents are inspected. Add {@code docs/public-api.md}
     * here when Phase 9 of {@code JAVA_API_1_0_GOAL.md} writes it.
     */
    @Test
    void currentFacingDocumentationUsesOnlySupportedJavaNames() throws IOException {
        List<String> failures = new ArrayList<>();
        for (Path path : List.of(Path.of("README.md"))) {
            inspectCurrentDocument(path, failures);
        }
        assertNoFailures(failures);
    }

    private static void inspectTree(Path root, boolean inspectInterfaceDeclarations, List<String> failures)
            throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            paths.filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> !path.getFileName().toString().equals(SELF))
                    .sorted()
                    .forEach(path -> {
                        try {
                            String source = Files.readString(path);
                            inspectText(path, source, failures);
                            if (inspectInterfaceDeclarations
                                    && I_PREFIX_INTERFACE.matcher(source).find()) {
                                failures.add(path + ": declares an I-prefixed interface");
                            }
                        } catch (IOException exception) {
                            failures.add(path + ": " + exception.getMessage());
                        }
                    });
        }
    }

    private static void inspectCurrentDocument(Path path, List<String> failures) throws IOException {
        int lineNumber = 0;
        for (String line : Files.readAllLines(path)) {
            lineNumber++;
            if (line.contains("C#")) {
                continue;
            }
            inspectText(Path.of(path + ":" + lineNumber), line, failures);
        }
    }

    private static void inspectText(Path source, String text, List<String> failures) {
        for (Pattern pattern : STALE_JAVA_NAMES) {
            if (pattern.matcher(text).find()) {
                failures.add(source + ": matches " + pattern);
            }
        }
    }

    private static void assertNoFailures(List<String> failures) {
        assertTrue(failures.isEmpty(), () -> String.join(System.lineSeparator(), failures));
    }
}
