// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.common.WaitKey;
import dev.slsk.common.Waiter;
import dev.slsk.diagnostics.DiagnosticSink;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.options.SoulseekClientOptions;
import java.util.concurrent.CompletableFuture;

/**
 * The seam between the client and the components it delegates to.
 *
 * <p>{@code DefaultSoulseekClient} was one class of five thousand lines owning
 * connection lifecycle, the server protocol, searches, transfers, user queries,
 * rooms and semaphore garbage collection at once. Splitting it needs somewhere
 * for the parts each component genuinely shares — the server write path, the
 * response correlator, login state — to live without handing every component a
 * reference to the whole client.
 *
 * <p>Deliberately small. A component that needs more than this is a sign the
 * split is in the wrong place, not that the interface should grow.
 */
interface ClientContext {

    /** Returns the response correlator. */
    Waiter getWaiter();

    /** Returns the client's options. */
    SoulseekClientOptions getClientOptions();

    /** Returns the diagnostic sink. */
    DiagnosticSink getDiagnostic();

    /**
     * Throws unless the client is connected and logged in.
     *
     * @param operation what the caller is trying to do, for the message
     */
    void requireLoggedIn(String operation);

    /**
     * Substitutes the client's default signal when the caller supplied none.
     *
     * @param cancellationSignal the caller's signal, possibly {@code null}
     * @return a signal, never {@code null}
     */
    CancellationSignal defaultToken(CancellationSignal cancellationSignal);

    /**
     * Sends a command and waits for the server's acknowledgement.
     *
     * <p>The dominant shape of a server operation: register a correlated wait,
     * write, translate the failure. Shared here so each component does not
     * reimplement it.
     *
     * @param message the command to send
     * @param waitKey correlates the acknowledgement
     * @param cancellationSignal the cancellation signal
     * @param failurePrefix prefixes any wrapped failure
     * @return a future completing when the server acknowledges
     */
    CompletableFuture<Void> executeCorrelatedCommand(
            OutgoingMessage message, WaitKey waitKey, CancellationSignal cancellationSignal, String failurePrefix);

    /**
     * Sends a request and waits for a typed response.
     *
     * @param message the request to send
     * @param waitKey correlates the response
     * @param resultType the expected response type
     * @param cancellationSignal the cancellation signal
     * @param failurePrefix prefixes any wrapped failure
     * @param preservedFailures failure types to pass through untranslated
     * @param <T> the response type
     * @return a future containing the response
     */
    <T> CompletableFuture<T> executeCorrelatedRequest(
            OutgoingMessage message,
            WaitKey waitKey,
            Class<T> resultType,
            CancellationSignal cancellationSignal,
            String failurePrefix,
            Class<? extends Throwable>... preservedFailures);

    /**
     * Returns this client's own logged-in username, or {@code null}.
     *
     * <p>Needed because several failure messages name the logged-in user rather
     * than the user the request was about — {@code "Failed to retrieve
     * statistics for user <me>"}. That reads like a bug and is not: upstream
     * interpolates {@code Username} at exactly these sites, so behavioural
     * parity means keeping it.
     *
     * @return the logged-in username
     */
    String getLoggedInUsername();

    /**
     * Returns the live search registry.
     *
     * <p>Stays on the client because incoming distributed and peer messages are
     * dispatched against it from the message handlers, which the client owns.
     *
     * @return searches by token
     */
    java.util.Map<Integer, dev.slsk.search.SearchInternal> getSearchRegistry();

    /** Returns the token allocator. */
    dev.slsk.common.TokenFactory getTokenFactory();

    /** Returns the client's shared timer. */
    dev.slsk.common.Scheduler getScheduler();

    /**
     * Writes pre-encoded bytes to the server connection.
     *
     * @param message the encoded message
     * @param cancellationSignal the cancellation signal
     * @return a future completing when the write lands
     */
    CompletableFuture<Void> writeBytesToServer(byte[] message, CancellationSignal cancellationSignal);

    /**
     * Raises a search-related client event.
     *
     * @param event which event
     * @param eventData the payload
     * @param <T> the payload type
     */
    <T> void raiseSearchEvent(DefaultSoulseekClient.Event event, T eventData);

    /** Returns the peer connection manager. */
    dev.slsk.network.PeerConnectionManager getPeerConnectionManager();

    /**
     * Resolves a user's endpoint, honouring the endpoint cache.
     *
     * @param username the user to resolve
     * @param cancellationSignal the cancellation signal
     * @return a future containing the endpoint
     */
    CompletableFuture<java.net.InetSocketAddress> resolveUserEndpoint(
            String username, CancellationSignal cancellationSignal);

    /**
     * Raises a browse-progress event.
     *
     * <p>Event listener lists belong to the client, so a component reports
     * progress rather than raising it directly.
     *
     * @param username the user being browsed
     * @param options the browse options in force
     * @param currentBytes bytes received so far
     * @param totalBytes bytes expected
     * @param completed set once the browse has finished
     */
    void reportBrowseProgress(
            String username,
            dev.slsk.options.BrowseOptions options,
            long currentBytes,
            long totalBytes,
            java.util.concurrent.atomic.AtomicBoolean completed);

    /**
     * Writes a message to an arbitrary peer connection.
     *
     * @param connection the connection to write to
     * @param message the message to send
     * @param cancellationSignal the cancellation signal
     * @return a future completing when the write lands
     */
    CompletableFuture<Void> writeToPeer(
            dev.slsk.network.MessageConnection connection,
            OutgoingMessage message,
            CancellationSignal cancellationSignal);

    /**
     * Writes a message to the server connection.
     *
     * @param message the message to send
     * @param cancellationSignal the cancellation signal
     * @return a future completing when the write lands
     */
    CompletableFuture<Void> writeToServer(OutgoingMessage message, CancellationSignal cancellationSignal);
}
