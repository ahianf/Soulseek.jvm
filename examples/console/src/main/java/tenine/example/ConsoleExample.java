// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package tenine.example;

import dev.slsk.CancellationSignal;
import dev.slsk.SearchQuery;
import dev.slsk.SearchResponse;
import dev.slsk.SoulseekClient;
import dev.slsk.Transfer;
import dev.slsk.diagnostics.DiagnosticEvent;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A small interactive Soulseek search and download client.
 */
public final class ConsoleExample {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleExample.class);
    /**
     * Diagnostics raised by the library itself, logged under the library package so the log
     * configuration can tune protocol detail separately from this example's own messages.
     */
    private static final Logger DIAGNOSTICS = LoggerFactory.getLogger("dev.slsk.diagnostics");

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
                LOG.info("downloads will be saved to {}", DOWNLOAD_DIRECTORY);
                LOG.info("listening for peers on port {}; forward this TCP port in your router/firewall", listenPort);

                client.addTransferStateChangedListener((sender, event) -> LOG.info(
                        "transfer state for {}: {} -> {}",
                        event.getTransfer().getFilename(),
                        event.getPreviousState(),
                        event.getTransfer().getState()));
                client.addTransferProgressUpdatedListener((sender, event) -> logProgress(event));
                client.addDiagnosticGeneratedListener((sender, event) -> logDiagnostic(event));
                LOG.info("connecting as {}", username);
                client.connectAsync(username, password).join();
                LOG.info(
                        "logged in as {} (network version {}.{})",
                        client.getUsername(),
                        client.getMajorVersion(),
                        client.getMinorVersion());

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
        LOG.info(
                "searching '{}'; results appear live and finish after {} seconds of silence",
                query,
                SEARCH_TIMEOUT_MILLISECONDS / 1_000);
        var search = client.searchAsync(
                        SearchQuery.fromText(query),
                        response -> addSearchFiles(response, files),
                        null,
                        null,
                        new SearchOptions(SEARCH_TIMEOUT_MILLISECONDS, SEARCH_RESPONSE_LIMIT),
                        CancellationSignal.none())
                .join();
        LOG.info(
                "search finished: {}. {} public file(s) accepted; {} shown for selection",
                search.getState(),
                search.getFileCount(),
                files.size());
        if (files.isEmpty()) {
            LOG.warn("no downloadable public files found; an unforwarded listening port reduces peer responses");
            return;
        }

        int selected = selectedIndex(input, files.size());
        if (selected < 0) {
            return;
        }
        SearchFile file = files.get(selected);
        Path destination = availableDestination(localFilename(file.remoteFilename()));
        LOG.info("requesting {} from {} to {}", file.remoteFilename(), file.username(), destination);
        try {
            var completion = client.enqueueDownloadAsync(
                            file.username(), file.remoteFilename(), destination.toString(), file.size(), 0, null, null)
                    .join();
            LOG.info("peer accepted the request; waiting for the transfer to finish");
            Transfer transfer = completion.join();
            LOG.info("download finished: {}", transfer.getState());
        } catch (CompletionException exception) {
            LOG.error(
                    "download of {} from {} failed: {}",
                    file.remoteFilename(),
                    file.username(),
                    failureMessage(exception),
                    exception);
            LOG.warn("the peer may be offline, busy, or not accepting transfers; try another result");
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
                LOG.info(
                        "[{}] {} — {} ({}; {}, queue {}, {} KiB/s)",
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
            LOG.warn("enter a number from 1 to {}", limit);
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
            LOG.warn("enter a port from 1024 to 65535");
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

    private static void logProgress(TransferProgressUpdatedEvent event) {
        // Progress fires once per transferred chunk, so skip the formatting when debug is off.
        if (!LOG.isDebugEnabled()) {
            return;
        }
        Transfer transfer = event.getTransfer();
        LOG.debug(
                "progress for {}: {} ({}/{})",
                transfer.getFilename(),
                String.format("%.1f%%", transfer.getPercentComplete()),
                humanSize(transfer.getBytesTransferred()),
                humanSize(transfer.getSize()));
    }

    /**
     * Mirrors a diagnostic raised by the library onto the matching log level, so the whole
     * conversation with the server and with peers is visible while the client is connected.
     */
    private static void logDiagnostic(DiagnosticEvent event) {
        String message = event.getMessage();
        Throwable exception = event.getException();
        if (exception != null) {
            DIAGNOSTICS.error(message, exception);
            return;
        }
        switch (event.getLevel()) {
            case WARNING -> DIAGNOSTICS.warn(message);
            case INFO -> DIAGNOSTICS.info(message);
            case DEBUG -> DIAGNOSTICS.debug(message);
            case TRACE -> DIAGNOSTICS.trace(message);
            case NONE -> {
                // Nothing to report.
            }
            default -> DIAGNOSTICS.info(message);
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
