// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package tenine.example;

import dev.slsk.CancellationSignal;
import dev.slsk.SearchQuery;
import dev.slsk.SearchResponse;
import dev.slsk.SoulseekClient;
import dev.slsk.Transfer;
import dev.slsk.diagnostics.DiagnosticLevel;
import dev.slsk.events.TransferProgressUpdatedEvent;
import dev.slsk.options.SearchOptions;
import dev.slsk.options.SoulseekClientOptions;
import java.io.Console;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CompletionException;

/**
 * A small interactive Soulseek search and download client.
 */
public final class ConsoleExample {
    private static final Path DOWNLOAD_DIRECTORY = Path.of(System.getProperty("user.home"), "slsk-jvm", "downloads");
    // slskd uses minor version 760 with the same major protocol version (170).
    private static final int MINOR_VERSION = 760;
    private static final int LISTEN_PORT = 50_000;
    private static final int MESSAGE_TIMEOUT_MILLISECONDS = 15_000;
    private static final int SEARCH_TIMEOUT_MILLISECONDS = 15_000;
    private static final int SEARCH_RESPONSE_LIMIT = 5_000;
    private static final int VISIBLE_RESULT_LIMIT = 100;

    private ConsoleExample() {}

    /** Runs the interactive CLI. */
    public static void main(String[] args) throws Exception {
        try (Scanner input = new Scanner(System.in)) {
            int listenPort = listeningPort(input);
            SoulseekClientOptions options = new SoulseekClientOptions(
                    true, null, listenPort, MESSAGE_TIMEOUT_MILLISECONDS, DiagnosticLevel.DEBUG);
            try (SoulseekClient client = SoulseekClient.create(MINOR_VERSION, options)) {
                String username = required(input, "Soulseek username: ");
                String password = password(input);
                Files.createDirectories(DOWNLOAD_DIRECTORY);
                System.out.println("Downloads will be saved to " + DOWNLOAD_DIRECTORY);
                System.out.printf(
                        "Listening for peers on port %d; forward this TCP port in your router/firewall.%n", listenPort);

                client.addTransferStateChangedListener((sender, event) -> System.out.printf(
                        "%nTransfer state for %s: %s -> %s%n",
                        event.getTransfer().getFilename(),
                        event.getPreviousState(),
                        event.getTransfer().getState()));
                client.addTransferProgressUpdatedListener((sender, event) -> printProgress(event));
                client.addDiagnosticGeneratedListener((sender, event) -> printConnectionDiagnostic(event.getMessage()));
                System.out.println("Connecting...");
                client.connectAsync(username, password).join();
                System.out.printf(
                        "Logged in as %s (network version %d.%d).%n",
                        client.getUsername(), client.getMajorVersion(), client.getMinorVersion());

                while (true) {
                    String query = prompt(input, "\nSearch (blank to quit): ");
                    if (query.isBlank()) {
                        return;
                    }
                    searchAndOfferDownload(client, input, query);
                }
            }
        }
    }

    private static void searchAndOfferDownload(SoulseekClient client, Scanner input, String query) {
        List<SearchFile> files = new ArrayList<>();
        System.out.printf(
                "Searching; results appear live and finish after %d seconds of silence.%n",
                SEARCH_TIMEOUT_MILLISECONDS / 1_000);
        var search = client.searchAsync(
                        SearchQuery.fromText(query),
                        response -> addSearchFiles(response, files),
                        null,
                        null,
                        new SearchOptions(SEARCH_TIMEOUT_MILLISECONDS, SEARCH_RESPONSE_LIMIT),
                        CancellationSignal.none())
                .join();
        System.out.printf(
                "Search finished: %s. %d public file(s) accepted; %d shown for selection.%n",
                search.getState(), search.getFileCount(), files.size());
        if (files.isEmpty()) {
            System.out.println("No downloadable public files found.");
            return;
        }

        int selected = selectedIndex(input, files.size());
        if (selected < 0) {
            return;
        }
        SearchFile file = files.get(selected);
        Path destination = availableDestination(localFilename(file.remoteFilename()));
        System.out.printf("Requesting %s from %s to %s%n", file.remoteFilename(), file.username(), destination);
        try {
            var completion = client.enqueueDownloadAsync(
                            file.username(), file.remoteFilename(), destination.toString(), file.size(), 0, null, null)
                    .join();
            System.out.println("Peer accepted the request; waiting for the transfer to finish.");
            Transfer transfer = completion.join();
            System.out.printf("Download finished: %s%n", transfer.getState());
        } catch (CompletionException exception) {
            System.out.println();
            System.err.println("Download failed: " + failureMessage(exception));
            System.err.println("The peer may be offline, busy, or not accepting transfers. Try another result.");
        }
    }

    private static void addSearchFiles(SearchResponse response, List<SearchFile> files) {
        synchronized (files) {
            for (dev.slsk.File file : response.getFiles()) {
                if (files.size() >= VISIBLE_RESULT_LIMIT) {
                    return;
                }
                SearchFile result = new SearchFile(
                        response.getUsername(),
                        file.getFilename(),
                        file.getSize(),
                        response.hasFreeUploadSlot(),
                        response.getQueueLength(),
                        response.getUploadSpeed());
                files.add(result);
                System.out.printf(
                        "[%d] %s — %s (%s; %s, queue %d, %d KiB/s)%n",
                        files.size(),
                        result.username(),
                        result.remoteFilename(),
                        humanSize(result.size()),
                        result.freeUploadSlot() ? "free slot" : "no free slot",
                        result.queueLength(),
                        result.uploadSpeed() / 1024);
            }
        }
    }

    private static int selectedIndex(Scanner input, int limit) {
        while (true) {
            String value = prompt(input, "Download number (blank to search again): ");
            if (value.isBlank()) {
                return -1;
            }
            try {
                int selected = Integer.parseInt(value);
                if (selected >= 1 && selected <= limit) {
                    return selected - 1;
                }
            } catch (NumberFormatException ignored) {
                // Repeat the prompt below.
            }
            System.out.printf("Enter a number from 1 to %d.%n", limit);
        }
    }

    private static int listeningPort(Scanner input) {
        while (true) {
            String value = prompt(input, "Listening port [" + LISTEN_PORT + "]: ");
            if (value.isBlank()) {
                return LISTEN_PORT;
            }
            try {
                int port = Integer.parseInt(value);
                if (port >= 1024 && port <= 65_535) {
                    return port;
                }
            } catch (NumberFormatException ignored) {
                // Repeat the prompt below.
            }
            System.out.println("Enter a port from 1024 to 65535.");
        }
    }

    private static String password(Scanner input) {
        Console console = System.console();
        if (console != null) {
            char[] password = console.readPassword("Soulseek password: ");
            return password == null ? "" : new String(password);
        }
        return required(input, "Soulseek password (input will be visible): ");
    }

    private static String required(Scanner input, String label) {
        String value = prompt(input, label);
        if (value.isBlank()) {
            throw new IllegalArgumentException(label.strip() + " is required");
        }
        return value;
    }

    private static String prompt(Scanner input, String label) {
        System.out.print(label);
        return input.hasNextLine() ? input.nextLine().strip() : "";
    }

    private static String localFilename(String remoteFilename) {
        String normalized = remoteFilename.replace('\\', '/');
        Path filename = Path.of(normalized).getFileName();
        if (filename == null || filename.toString().isBlank()) {
            throw new IllegalArgumentException("The selected remote filename has no file name");
        }
        return filename.toString();
    }

    private static Path availableDestination(String filename) {
        Path destination = DOWNLOAD_DIRECTORY.resolve(filename);
        if (!Files.exists(destination)) {
            return destination;
        }
        int extensionIndex = filename.lastIndexOf('.');
        String stem = extensionIndex > 0 ? filename.substring(0, extensionIndex) : filename;
        String extension = extensionIndex > 0 ? filename.substring(extensionIndex) : "";
        for (int copy = 2; ; copy++) {
            destination = DOWNLOAD_DIRECTORY.resolve(stem + " (" + copy + ")" + extension);
            if (!Files.exists(destination)) {
                return destination;
            }
        }
    }

    private static String humanSize(long bytes) {
        if (bytes < 1024 * 1024) {
            return String.format("%.1f KiB", bytes / 1024.0);
        }
        return String.format("%.1f MiB", bytes / (1024.0 * 1024.0));
    }

    private static String failureMessage(Throwable failure) {
        Throwable cause = failure;
        while (cause.getCause() != null && cause != cause.getCause()) {
            cause = cause.getCause();
        }
        return cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
    }

    private static void printProgress(TransferProgressUpdatedEvent event) {
        Transfer transfer = event.getTransfer();
        System.out.printf(
                "\rProgress: %.1f%% (%s/%s)",
                transfer.getPercentComplete(),
                humanSize(transfer.getBytesTransferred()),
                humanSize(transfer.getSize()));
        if (transfer.getPercentComplete() >= 100) {
            System.out.println();
        }
    }

    private static void printConnectionDiagnostic(String message) {
        if (message != null
                && (message.contains("GET_PEER_ADDRESS")
                        || message.contains("endpoint response")
                        || message.contains("message connection")
                        || message.contains("Soliciting indirect"))) {
            System.out.println("[network] " + message);
        }
    }

    private record SearchFile(
            String username,
            String remoteFilename,
            long size,
            boolean freeUploadSlot,
            int queueLength,
            int uploadSpeed) {}
}
