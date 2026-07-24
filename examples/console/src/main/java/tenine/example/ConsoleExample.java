// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package tenine.example;

import dev.slsk.BrowseResponse;
import dev.slsk.SearchQuery;
import dev.slsk.SearchResponse;
import dev.slsk.SearchResult;
import dev.slsk.SoulseekClient;
import dev.slsk.Transfer;
import dev.slsk.events.TransferProgressUpdatedEvent;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

/**
 * Demonstrates connection, search, browse, and download lifecycles using only
 * the exported Soulseek.jvm API.
 */
public final class ConsoleExample {
    private static final String USERNAME_VARIABLE = "SLSK_USERNAME";
    private static final String PASSWORD_VARIABLE = "SLSK_PASSWORD";
    private static final String MINOR_VERSION_VARIABLE = "SLSK_MINOR_VERSION";

    private ConsoleExample() {}

    /**
     * Runs the example.
     *
     * @param args search text, optional browse user, and optional remote and
     *             local download filenames
     */
    public static void main(String[] args) {
        if (args.length < 1 || args.length == 3 || args.length > 4) {
            printUsage();
            return;
        }

        String username = requiredEnvironmentVariable(USERNAME_VARIABLE);
        String password = requiredEnvironmentVariable(PASSWORD_VARIABLE);
        int minorVersion = uniqueMinorVersion();

        try (SoulseekClient client = SoulseekClient.create(minorVersion)) {
            registerLifecycleOutput(client);

            client.connectAsync(username, password).join();
            System.out.printf(
                    "Logged in as %s with network version %d.%d%n",
                    client.getUsername(), client.getMajorVersion(), client.getMinorVersion());

            SearchResult searchResult =
                    client.searchAsync(SearchQuery.fromText(args[0])).join();
            List<SearchResponse> responses = searchResult.responses().stream()
                    .sorted(Comparator.comparing(SearchResponse::isHasFreeUploadSlot)
                            .reversed()
                            .thenComparing(SearchResponse::getUploadSpeed, Comparator.reverseOrder()))
                    .toList();
            int fileCount =
                    responses.stream().mapToInt(SearchResponse::getFileCount).sum();
            System.out.printf("Search completed: %d file(s) from %d user(s).%n", fileCount, responses.size());

            String browseUsername = args.length >= 2
                    ? args[1]
                    : responses.stream()
                            .findFirst()
                            .map(SearchResponse::getUsername)
                            .orElse(null);
            if (browseUsername == null) {
                System.out.println("No search response is available to browse.");
                return;
            }

            BrowseResponse browse = client.browseAsync(browseUsername).join();
            System.out.printf(
                    "Browse completed for %s: %d public and %d locked " + "director(ies).%n",
                    browseUsername, browse.getDirectoryCount(), browse.getLockedDirectoryCount());

            if (args.length == 4) {
                String remoteFilename = args[2];
                String localFilename =
                        Path.of(args[3]).toAbsolutePath().normalize().toString();
                Transfer completed = client.downloadAsync(browseUsername, remoteFilename, localFilename)
                        .join();
                System.out.printf("Download completed in state %s: %s%n", completed.getState(), localFilename);
            } else {
                System.out.println("Supply REMOTE_FILENAME and LOCAL_FILENAME to run the " + "download lifecycle.");
            }
        }
    }

    private static void registerLifecycleOutput(SoulseekClient client) {
        client.addStateChangedListener((sender, event) -> System.out.printf(
                "Client state: %s -> %s%s%n",
                event.getPreviousState(),
                event.getState(),
                event.getMessage() == null ? "" : " (" + event.getMessage() + ")"));
        client.addBrowseProgressUpdatedListener((sender, event) ->
                System.out.printf("Browse progress for %s: %.1f%%%n", event.getUsername(), event.getPercentComplete()));
        client.addTransferStateChangedListener((sender, event) -> System.out.printf(
                "Transfer state for %s: %s -> %s%n",
                event.getTransfer().getFilename(),
                event.getPreviousState(),
                event.getTransfer().getState()));
        client.addTransferProgressUpdatedListener((sender, event) -> printTransferProgress(event));
    }

    private static void printTransferProgress(TransferProgressUpdatedEvent event) {
        Transfer transfer = event.getTransfer();
        System.out.printf(
                "Transfer progress for %s: %d/%d bytes (%.1f%%)%n",
                transfer.getFilename(),
                transfer.getBytesTransferred(),
                transfer.getSize(),
                transfer.getPercentComplete());
    }

    private static int uniqueMinorVersion() {
        String value = requiredEnvironmentVariable(MINOR_VERSION_VARIABLE);
        int parsed;
        try {
            parsed = Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    MINOR_VERSION_VARIABLE + " must be an integer greater than 100", exception);
        }
        if (parsed <= 100) {
            throw new IllegalArgumentException(MINOR_VERSION_VARIABLE + " must be greater than 100");
        }
        return parsed;
    }

    private static String requiredEnvironmentVariable(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Required environment variable " + name + " is not set");
        }
        return value;
    }

    private static void printUsage() {
        System.out.println("Usage: ConsoleExample SEARCH_TEXT [BROWSE_USER " + "[REMOTE_FILENAME LOCAL_FILENAME]]");
        System.out.println("Set SLSK_USERNAME, SLSK_PASSWORD, and SLSK_MINOR_VERSION first.");
    }
}
