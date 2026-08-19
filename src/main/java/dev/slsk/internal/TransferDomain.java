// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.exceptions.DuplicateTokenException;
import dev.slsk.exceptions.DuplicateTransferException;
import dev.slsk.exceptions.TransferNotFoundException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.internal.common.CommonUtils;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.IOAdapter;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.common.Scheduler;
import dev.slsk.internal.common.TokenBucket;
import dev.slsk.internal.common.TokenFactory;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.concurrent.CancellationController;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.handlers.PeerServices;
import dev.slsk.internal.messaging.messages.PlaceInQueueRequest;
import dev.slsk.internal.messaging.messages.PlaceInQueueResponse;
import dev.slsk.internal.messaging.messages.TransferRequest;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.PeerConnectionManager;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.options.TransferOptions;
import dev.slsk.internal.transfer.DownloadRequest;
import dev.slsk.internal.transfer.Transfer;
import dev.slsk.internal.transfer.TransferDirection;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.internal.transfer.TransferState;
import dev.slsk.internal.transfer.TransferStreams;
import dev.slsk.internal.transfer.UploadRequest;
import dev.slsk.spi.ResolvedFile;
import dev.slsk.spi.ShareCatalog;
import dev.slsk.spi.UploadPolicy;
import dev.slsk.transfer.TransferId;
import dev.slsk.transfer.TransferOutcome;
import dev.slsk.user.UserProfile;
import dev.slsk.user.Username;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.LongFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Everything about moving bytes to and from a peer, and everything a peer can
 * ask of us on the way.
 *
 * <p>This is the decide side of the transfer system, in the shape
 * {@link DownloadQueue} already had: this class decides — whether a request is
 * a duplicate, which token it gets, when a slot is free, who may upload and in
 * what order — and a {@link DownloadRun} or {@link UploadRun} does, blocking on
 * the caller's own virtual thread until the transfer reaches a terminal state.
 *
 * <p>It replaces {@code TransferEngine}, which held the same rules but ran them
 * through a future the caller immediately joined, and which reached everything it needed through the engine
 * that owned everything. What is different is ownership: the two registries,
 * both concurrency limits, the per-user upload semaphores, the duplicate keys
 * and the running uploads' cancellation live here, in one place, rather than in
 * two copies split between the engine and the transfer engine.
 *
 * <p>It answers {@link PeerServices}, which the engine used to. Two of those
 * six members it does not own: the share catalog belongs to {@code Shares} and
 * the profile to {@code Me}, and this reads them because serving a peer needs
 * both. The other four — the upload policy, the admission, serving a file, and
 * what an unsolicited offer turned out to be — are transfers, and they are
 * owned here.
 */
final class TransferDomain implements PeerServices {

    /** What the transfer path reads its context from, named rather than "the engine". */
    private final Supplier<SoulseekClientOptions> options;

    final DiagnosticSink diagnostic;
    final Waiter waiter;
    private final Supplier<PeerConnectionManager> peers;
    private final EndpointResolver endpoints;
    private final ServerLink server;
    private final TokenFactory tokens;
    private final IOAdapter io;
    final TokenBucket downloadTokenBucket;
    final TokenBucket uploadTokenBucket;
    private final Supplier<ShareCatalog> catalog;
    private final Supplier<UserProfile> profile;

    /**
     * Global transfer concurrency limits.
     *
     * <p>There was one pair of these here and an identical, unread pair on the
     * engine, along with a per-user map the engine swept and the upload path
     * never used. One owner now, and the sweep reaches the map that is real.
     */
    /**
     * How many downloads may move bytes at once, and how many per peer.
     *
     * <p>Replaceable, because {@link dev.slsk.download.DownloadPolicy} owns these
     * numbers and a consumer may change them while transfers are in flight. A
     * run holds the instance it acquired and releases to that same one, so a
     * resize never returns a permit to a semaphore that did not issue it.
     *
     * <p>They bound <em>transfers</em>, not places in a peer's queue. Under
     * QueueUpload a download waits in the peer's queue holding nothing but a
     * line in it — the peer message connection is shared — so the ceiling is
     * taken when the peer says it is ready, which is the first moment there is
     * a connection to be rude with.
     */
    private final java.util.concurrent.atomic.AtomicReference<Semaphore> globalDownloadSemaphore;

    private volatile int maximumDownloadsPerUser;

    private final Map<String, Semaphore> downloadSemaphores = new ConcurrentHashMap<>();

    private final Semaphore globalUploadSemaphore;

    /** Per-user upload limits, and the lock guarding their creation. */
    private final Map<String, Semaphore> uploadSemaphores = new ConcurrentHashMap<>();

    /** How many runs hold a reference to each user's semaphore; see {@link #uploadSemaphoreFor}. */
    private final Map<String, java.util.concurrent.atomic.AtomicInteger> uploadSemaphoreLeases =
            new ConcurrentHashMap<>();

    private final Semaphore uploadSemaphoreSyncRoot = new Semaphore(1);

    /** Duplicate-transfer keys; owned here, since this is what detects duplicates. */
    private final Map<String, Boolean> uniqueKeys = new ConcurrentHashMap<>();

    /**
     * The transfers in flight, by token.
     *
     * <p>They are here rather than on the engine because an inbound peer or
     * server message is dispatched against them and this is what puts them
     * there. Volatile and replaceable only for the test seam that swaps in a
     * prepared registry.
     */
    private volatile Map<Integer, TransferInternal> downloads = new ConcurrentHashMap<>();

    private volatile Map<Integer, TransferInternal> uploads = new ConcurrentHashMap<>();

    /**
     * How a running upload is stopped.
     *
     * <p>Uploads are started by us on a peer's behalf, so nothing outside holds
     * a signal for one. Keeping the controller here is what makes
     * {@code Uploads.cancel} able to do anything at all.
     */
    private final Map<TransferId, CancellationController> uploadCancellations = new ConcurrentHashMap<>();

    private volatile UploadPolicy uploadPolicy = UploadPolicy.standard(2, 1);
    private final UploadAdmission admission;
    private final UploadRetry uploadRetry;

    /**
     * What we tell peers our average upload speed is, in bytes per second.
     *
     * <p>The server's number, not a local estimate: every upload served is
     * reported to the server, which folds it into the account's average, and
     * this is that average as last heard — seeded after login, refreshed after
     * each report. It goes into every search response we send, which is what
     * peers rank us as a source by; left at zero we advertise ourselves as the
     * slowest source on the network.
     */
    private final java.util.concurrent.atomic.AtomicInteger advertisedUploadSpeed =
            new java.util.concurrent.atomic.AtomicInteger();

    /**
     * Who decides what an offered file is.
     *
     * <p>The download queue, once the downloads facet installs itself. Until
     * then every offer is unknown, which is the honest answer: without a queue
     * there is nothing an offer could match beyond the live transfers the
     * handler already checked.
     */
    private volatile DownloadOffers downloadOffers = (username, filename, offer) -> OfferDisposition.UNKNOWN;

    /**
     * Where a reported place-in-queue goes. The downloads facet plugs in here;
     * until it does there is no queue to record one against.
     */
    private volatile DownloadPositions downloadPositions = (username, filename, position) -> {};

    /** Resolves a peer's address. Owned by {@code UserDirectory}; see I5 in the goal. */
    @FunctionalInterface
    interface EndpointResolver {
        InetSocketAddress resolve(String username, CancellationSignal cancellationSignal);
    }

    /** Answers a peer's unsolicited offer of a file. */
    @FunctionalInterface
    interface DownloadOffers {
        OfferDisposition offered(String username, String filename, TransferRequest offer);
    }

    /** Takes a place-in-queue a peer reported, asked for or not. */
    @FunctionalInterface
    interface DownloadPositions {
        void reported(String username, String filename, int position);
    }

    TransferDomain(
            Supplier<SoulseekClientOptions> options,
            DiagnosticSink diagnostic,
            Waiter waiter,
            Supplier<PeerConnectionManager> peers,
            EndpointResolver endpoints,
            ServerLink server,
            TokenFactory tokens,
            IOAdapter io,
            TokenBucket downloadTokenBucket,
            TokenBucket uploadTokenBucket,
            Supplier<ShareCatalog> catalog,
            Supplier<UserProfile> profile,
            Predicate<String> privileged,
            Scheduler scheduler) {
        this.options = Objects.requireNonNull(options, "options");
        this.diagnostic = Objects.requireNonNull(diagnostic, "diagnostic");
        this.waiter = Objects.requireNonNull(waiter, "waiter");
        this.peers = Objects.requireNonNull(peers, "peers");
        this.endpoints = Objects.requireNonNull(endpoints, "endpoints");
        this.server = Objects.requireNonNull(server, "server");
        this.tokens = Objects.requireNonNull(tokens, "tokens");
        this.io = Objects.requireNonNull(io, "io");
        this.downloadTokenBucket = Objects.requireNonNull(downloadTokenBucket, "downloadTokenBucket");
        this.uploadTokenBucket = Objects.requireNonNull(uploadTokenBucket, "uploadTokenBucket");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.profile = Objects.requireNonNull(profile, "profile");
        this.globalDownloadSemaphore = new java.util.concurrent.atomic.AtomicReference<>(
                new Semaphore(options.get().getMaximumConcurrentDownloads()));
        this.maximumDownloadsPerUser = Integer.MAX_VALUE;
        this.globalUploadSemaphore = new Semaphore(options.get().getMaximumConcurrentUploads());
        this.admission = new UploadAdmission(
                this::uploadPolicy,
                this::uploads,
                Objects.requireNonNull(privileged, "privileged"),
                tokens::nextToken,
                diagnostic);
        this.uploadRetry = new UploadRetry(
                Objects.requireNonNull(scheduler, "scheduler"),
                UploadRetry.DELAY,
                UploadRetry.MAX_ATTEMPTS,
                this::reofferFailedUpload,
                diagnostic);
    }

    // --- what the runs read ------------------------------------------------

    SoulseekClientOptions clientOptions() {
        return options.get();
    }

    PeerConnectionManager peers() {
        return peers.get();
    }

    InetSocketAddress endpoint(String username, CancellationSignal cancellationSignal) {
        return endpoints.resolve(username, cancellationSignal);
    }

    Semaphore globalDownloadSemaphore() {
        return globalDownloadSemaphore.get();
    }

    /**
     * Returns the ceiling on concurrent transfers from one peer.
     *
     * @param username the peer
     * @return its semaphore, created on first use
     */
    Semaphore downloadSemaphoreFor(String username) {
        return downloadSemaphores.computeIfAbsent(username, ignored -> new Semaphore(maximumDownloadsPerUser));
    }

    /**
     * Applies a download policy's concurrency ceilings.
     *
     * <p>The policy's to set and the engine's to apply, the same division the
     * rate ceiling already follows: the queue decides which downloads exist and
     * these decide how many of them may be moving bytes at once.
     *
     * <p>Existing semaphores are dropped rather than resized. A run that already
     * holds a permit releases it to the instance it took it from, so the only
     * cost of a change mid-flight is that the new ceiling counts from zero.
     *
     * @param overall how many downloads may transfer at once
     * @param perUser how many may transfer at once from any one peer
     */
    void downloadConcurrency(int overall, int perUser) {
        globalDownloadSemaphore.set(new Semaphore(overall));
        maximumDownloadsPerUser = perUser;
        downloadSemaphores.clear();
    }

    Semaphore globalUploadSemaphore() {
        return globalUploadSemaphore;
    }

    /**
     * Returns this peer's upload semaphore, creating it under the sync root.
     *
     * <p>Soulseek NS cannot handle concurrent downloads from one source, so the
     * cap is enforced here whatever a consumer's policy says.
     *
     * @param username the peer
     * @param cancellationSignal stops the wait for the sync root
     * @return the peer's semaphore
     */
    Semaphore uploadSemaphoreFor(String username, CancellationSignal cancellationSignal) throws InterruptedException {
        dev.slsk.internal.common.Permits.acquire(uploadSemaphoreSyncRoot, cancellationSignal);
        try {
            // The lease is what makes the sweep safe. The C# source starts the
            // semaphore wait inside this sync root, so its sweep can never see
            // a fetched-but-unwaited semaphore; the blocking shape acquires
            // later, on the caller's own thread, and between fetch and acquire
            // the semaphore sits at full permits — exactly what the sweep
            // removes. Removing it hands the next upload to the same user a
            // fresh semaphore, and two uploads run concurrently against one
            // peer, which Soulseek NS cannot handle. The lease says "a run
            // holds a reference", permits or not.
            uploadSemaphoreLeases
                    .computeIfAbsent(username, ignored -> new java.util.concurrent.atomic.AtomicInteger())
                    .incrementAndGet();
            return uploadSemaphores.computeIfAbsent(
                    username, ignored -> new Semaphore(clientOptions().getMaximumConcurrentUploadsPerUser()));
        } finally {
            uploadSemaphoreSyncRoot.release();
        }
    }

    /** Returns a run's lease on its per-user semaphore; the sweep may then reclaim it. */
    void releaseUploadSemaphoreLease(String username) {
        java.util.concurrent.atomic.AtomicInteger leases = uploadSemaphoreLeases.get(username);
        if (leases != null) {
            leases.decrementAndGet();
        }
    }

    /** Drops the per-user upload semaphores nothing is holding or about to. */
    void cleanupUploadSemaphores() {
        if (!uploadSemaphoreSyncRoot.tryAcquire()) {
            return;
        }
        try {
            for (Map.Entry<String, Semaphore> entry : uploadSemaphores.entrySet()) {
                Semaphore semaphore = entry.getValue();
                if (semaphore.availablePermits() != clientOptions().getMaximumConcurrentUploadsPerUser()) {
                    continue;
                }
                java.util.concurrent.atomic.AtomicInteger leases = uploadSemaphoreLeases.get(entry.getKey());
                if (leases != null && leases.get() > 0) {
                    continue;
                }
                if (uploadSemaphores.remove(entry.getKey(), semaphore)) {
                    uploadSemaphoreLeases.remove(entry.getKey());
                    diagnostic.debug("Cleaned up upload semaphore for " + entry.getKey());
                }
            }
        } finally {
            uploadSemaphoreSyncRoot.release();
        }
    }

    /** Releases the duplicate-transfer key a finished run was holding. */
    void releaseUniqueKey(String key) {
        uniqueKeys.remove(key);
    }

    Map<Integer, TransferInternal> downloads() {
        return downloads;
    }

    Map<Integer, TransferInternal> uploads() {
        return uploads;
    }

    Map<String, Boolean> uniqueKeys() {
        return uniqueKeys;
    }

    void downloadsForTest(Map<Integer, TransferInternal> value) {
        downloads = value;
    }

    void uploadsForTest(Map<Integer, TransferInternal> value) {
        uploads = value;
    }

    Map<String, Semaphore> uploadSemaphoresForTest() {
        return uploadSemaphores;
    }

    Semaphore uploadSemaphoreSyncRootForTest() {
        return uploadSemaphoreSyncRoot;
    }

    // --- what a peer can ask of us ------------------------------------------

    @Override
    public ShareCatalog catalog() {
        return catalog.get();
    }

    @Override
    public UserProfile profile() {
        return profile.get();
    }

    @Override
    public UploadPolicy uploadPolicy() {
        return uploadPolicy;
    }

    /**
     * Sets who we serve and in what order.
     *
     * @param value the policy, or {@code null} for the standard one
     */
    void uploadPolicy(UploadPolicy value) {
        uploadPolicy = value == null ? UploadPolicy.standard(2, 1) : value;
    }

    @Override
    public UploadAdmission admission() {
        return admission;
    }

    @Override
    public int advertisedUploadSpeed() {
        return advertisedUploadSpeed.get();
    }

    /**
     * Adopts the server's upload average for this account.
     *
     * <p>Called when a statistics response names us; the engine listens for
     * those and routes ours here.
     *
     * @param bytesPerSecond the average the server reported
     */
    void advertisedUploadSpeed(int bytesPerSecond) {
        advertisedUploadSpeed.set(Math.max(0, bytesPerSecond));
    }

    void downloadOffers(DownloadOffers value) {
        this.downloadOffers = Objects.requireNonNull(value, "downloadOffers");
    }

    @Override
    public OfferDisposition offered(String username, String filename, TransferRequest offer) {
        return downloadOffers.offered(username, filename, offer);
    }

    void downloadPositions(DownloadPositions value) {
        this.downloadPositions = Objects.requireNonNull(value, "downloadPositions");
    }

    @Override
    public void queuePosition(String username, String filename, int position) {
        downloadPositions.reported(username, filename, position);
    }

    /**
     * Where the lifecycle of a served upload lands; the uploads facet plugs in
     * here.
     *
     * <p>Nothing published an {@code UploadEvent} before this existed: the
     * facet's bus was silent forever, and a finished upload vanished from
     * {@code all()} without a {@code Finished} ever firing — an upload was
     * observable only while in flight, and only by polling.
     */
    interface UploadObserver {
        /** An upload changed state; a null previous state means it just began. */
        void stateChanged(dev.slsk.internal.options.TransferStateChange change);

        /** Bytes moved. */
        void progressed(dev.slsk.internal.options.TransferProgressUpdate update);
    }

    private volatile UploadObserver uploadObserver;

    void uploadObserver(UploadObserver observer) {
        this.uploadObserver = Objects.requireNonNull(observer, "observer");
    }

    UploadObserver uploadObserverForTest() {
        return uploadObserver;
    }

    private void notifyUploadState(dev.slsk.internal.options.TransferStateChange change) {
        UploadObserver observer = uploadObserver;
        if (observer != null) {
            observer.stateChanged(change);
        }
    }

    private void notifyUploadProgress(dev.slsk.internal.options.TransferProgressUpdate update) {
        UploadObserver observer = uploadObserver;
        if (observer != null) {
            observer.progressed(update);
        }
    }

    /**
     * Serves a file to a peer whose request the policy allowed.
     *
     * <p>Nothing did this before 1.0. The old surface accepted the request in
     * {@code EnqueueDownloadCallback} and left the application to call
     * {@code upload(...)} itself, which is why "uploads are requested by peers
     * and admitted by the upload policy" has to mean the library serves them —
     * otherwise the capability is disposed of and nothing takes it over.
     *
     * <p>The bytes come from {@link ShareCatalog#resolve}, so a peer can only
     * ever receive what the catalog agrees it may have, checked against the
     * share root rather than trusted from the request.
     *
     * @param user who asked
     * @param path the file they asked for
     */
    @Override
    public void serve(Username user, String path) {
        serve(user, path, java.util.OptionalInt.empty());
    }

    /**
     * Serves a file, carrying the token a queued request already reserved.
     *
     * <p>A request that waited is served under the token it was given while it
     * waited, so the id the uploads facet reports does not change when the wait
     * ends. Minting a fresh one here made every queued upload two transfers to
     * anything watching: the queued one, which vanished without an outcome the
     * moment a slot freed, and the running one that replaced it.
     *
     * @param user who asked
     * @param path the file they asked for
     * @param reserved the token reserved while the request waited, if it waited
     */
    private void serve(Username user, String path, java.util.OptionalInt reserved) {
        NetworkExecutor.dispatch(
                () -> {
                    java.util.Optional<ResolvedFile> resolved;
                    try {
                        resolved = catalog().resolve(user, path);
                    } catch (RuntimeException failure) {
                        diagnostic.warning("The share catalog failed to resolve " + path, failure);
                        return;
                    }
                    if (resolved.isEmpty()) {
                        // Allowed by policy but not by the catalog. One answer for every
                        // rejection, so the reply cannot become a filesystem oracle.
                        admission.forget(user, path);
                        return;
                    }
                    ResolvedFile file = resolved.get();
                    // Stamps the round-robin counter at the moment the slot is
                    // taken, which is what "who has waited longest since their last
                    // upload started" is measured against.
                    admission.started(user);
                    int token = reserved.orElseGet(tokens::nextToken);
                    TransferId id = Transfers.uploadId(token);
                    CancellationController cancellation = new CancellationController();
                    uploadCancellations.put(id, cancellation);
                    // Already on a virtual thread of its own, so the upload is simply
                    // waited for.
                    try {
                        TransferOutcome outcome =
                                upload(UploadRequest.fromStream(user.value(), path, file.size(), offset -> {
                                            try {
                                                // Positioned rather than plain, so a resume can say
                                                // where it starts; see TransferStreams.positionedStream.
                                                return TransferStreams.positionedStream(file.open(offset), offset);
                                            } catch (IOException failure) {
                                                throw new UncheckedIOException(failure);
                                            }
                                        })
                                        .token(token)
                                        .cancellation(cancellation.getSignal())
                                        .options(new dev.slsk.internal.options.TransferOptions(
                                                this::notifyUploadState, this::notifyUploadProgress))
                                        .build());
                        if (outcome instanceof TransferOutcome.Succeeded succeeded) {
                            admission.served(user, succeeded.bytes());
                            uploadRetry.succeeded(user, path);
                            reportUploadSpeed(succeeded);
                        } else if (outcome instanceof TransferOutcome.Failed failed && failed.retryable()) {
                            // The peer was sent UploadFailed, which prompts a
                            // well-behaved client to re-queue on its own; the
                            // booking covers the peer that never comes back.
                            uploadRetry.failed(user, path);
                        }
                    } catch (RuntimeException failure) {
                        diagnostic.warning("Failed to serve an upload of " + path + " to " + user, failure);
                    } finally {
                        uploadCancellations.remove(id);
                        admission.forget(user, path);
                        // The slot this upload held is now free, so the queue moves.
                        // Nothing did this before: a queued peer waited until it
                        // asked again, and a peer that never re-asked waited for
                        // ever.
                        startNextQueued();
                    }
                },
                failure -> diagnostic.warning("Failed to serve " + path + " to " + user, failure));
    }

    /**
     * Tells the server how fast the upload that just finished ran.
     *
     * <p>The server folds each report into the account's average, which is the
     * speed peers rank us by. It does not push the refreshed average back, so
     * our own statistics are requested right after; the response arrives as a
     * statistics event naming us, and the engine routes it to
     * {@link #advertisedUploadSpeed(int)}.
     *
     * <p>Best-effort: the upload finished, and a failure to talk about it —
     * the server dropped between the last byte and here — must not turn a
     * served file into a diagnostic-worthy problem for the serve path.
     *
     * @param succeeded how the upload ended
     */
    private void reportUploadSpeed(TransferOutcome.Succeeded succeeded) {
        long nanos = succeeded.elapsed().toNanos();
        if (succeeded.bytes() <= 0 || nanos <= 0) {
            return;
        }
        long bytesPerSecond = (long) (succeeded.bytes() / (nanos / 1_000_000_000.0));
        if (bytesPerSecond <= 0) {
            return;
        }
        try {
            server.sendUploadSpeed((int) Math.min(bytesPerSecond, Integer.MAX_VALUE));
            server.write(
                    new dev.slsk.internal.messaging.messages.UserStatisticsRequest(server.username()),
                    CancellationSignal.none());
        } catch (RuntimeException failure) {
            diagnostic.debug("Failed to report the upload speed to the server: " + failure.getMessage());
        }
    }

    /**
     * Starts the next queued upload, if one may start.
     *
     * <p>The scheduler answers <em>who</em> is next; the upload policy answers
     * <em>whether</em> anyone may start at all, because it is the policy that
     * owns the slot count. Asking it here rather than duplicating the slot
     * accounting means there is one place that decides a slot is free.
     *
     * <p>Only one candidate is drawn per freed slot. If the policy still admits
     * more, the next completion draws the next one — a loop here would race the
     * admission's own view of what is running.
     */
    void startNextQueued() {
        java.util.Optional<dev.slsk.internal.transfer.UploadScheduler.Waiting> candidate;
        try {
            candidate = admission.next();
        } catch (RuntimeException failure) {
            diagnostic.warning("Failed to pick the next queued upload", failure);
            return;
        }
        if (candidate.isEmpty()) {
            return;
        }

        Username user = candidate.get().user();
        String path = candidate.get().path();
        if (admission.decide(user, path) instanceof dev.slsk.spi.UploadPolicy.Decision.Allow) {
            // With the token the request has worn since it was queued: the
            // decision above dropped it from the queue, and this is the same
            // request continuing, not a new one.
            serve(user, path, java.util.OptionalInt.of(candidate.get().token()));
        }
    }

    /**
     * Offers a failed upload to the admission again, when its retry comes due.
     *
     * <p>The peer may have re-asked on its own in the meantime — its fresh
     * request is the normal recovery — so a file that is queued or running
     * again makes this re-offer redundant and it is dropped. Otherwise the
     * re-offer takes the same path as a peer's request: the policy decides,
     * and a denial ends the retrying rather than booking another attempt
     * against the same answer.
     *
     * @param user who the failed upload was for
     * @param path the file that failed
     */
    private void reofferFailedUpload(Username user, String path) {
        if (admission.isQueued(user, path) || uploadActiveFor(user, path)) {
            return;
        }
        UploadPolicy.Decision decision;
        try {
            decision = admission.decide(user, path);
        } catch (RuntimeException failure) {
            diagnostic.warning("Failed to re-offer " + path + " to " + user, failure);
            return;
        }
        if (decision instanceof UploadPolicy.Decision.Allow) {
            serve(user, path);
        } else if (decision instanceof UploadPolicy.Decision.Queue) {
            // Queued by the decision itself; poke the pump in case no upload
            // is running to free a slot and draw it.
            startNextQueued();
        } else {
            uploadRetry.abandoned(user, path);
        }
    }

    /** Returns whether an upload of this file to this peer is running right now. */
    private boolean uploadActiveFor(Username user, String path) {
        for (TransferInternal upload : uploads().values()) {
            TransferState state = upload.getState();
            if (state == null || state.contains(TransferState.COMPLETED)) {
                continue;
            }
            if (user.value().equals(upload.getUsername()) && path.equals(upload.getFilename())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Stops a running upload. A no-op for one that has already finished.
     *
     * @param id which upload
     * @return whether there was one to stop
     */
    boolean cancelUpload(TransferId id) {
        CancellationController cancellation = uploadCancellations.remove(id);
        if (cancellation == null) {
            return false;
        }
        cancellation.cancel();
        return true;
    }

    /**
     * Records that a peer refused a download it had queued for us.
     *
     * <p>Every matching download, because a peer names the file rather than our
     * token and more than one attempt can be waiting on the same name.
     *
     * @param username who refused
     * @param filename what they refused
     * @param message what they said
     */
    void deniedByPeer(String username, String filename, String message) {
        for (TransferInternal download : matching(downloads, username, filename)) {
            download.settlement().fail(new dev.slsk.exceptions.TransferRejectedException(message));
            diagnostic.debug("Download of " + download.getFilename() + " from "
                    + download.getUsername() + " rejected by remote client (token: "
                    + download.getToken() + ")");
        }
    }

    /**
     * Records that a peer reported a download it was sending as failed.
     *
     * @param username who reported it
     * @param filename what failed
     */
    void failedByPeer(String username, String filename) {
        for (TransferInternal download : matching(downloads, username, filename)) {
            download.settlement()
                    .fail(new dev.slsk.exceptions.TransferReportedFailedException(
                            "Download reported as failed by remote client"));
            diagnostic.debug("Download of " + download.getFilename() + " from "
                    + download.getUsername() + " reported as failed by remote client (token: "
                    + download.getToken() + ")");
        }
    }

    private static java.util.List<TransferInternal> matching(
            Map<Integer, TransferInternal> registry, String username, String filename) {
        return registry.values().stream()
                .filter(transfer -> Objects.equals(transfer.getUsername(), username)
                        && Objects.equals(transfer.getFilename(), filename))
                .toList();
    }

    // --- intents ------------------------------------------------------------

    /**
     * Downloads what the request describes.
     *
     * <p>Blocks on the caller's own thread until the transfer reaches a
     * terminal state. What sat between this and the run below was a virtual
     * thread, a future, and a join on it from the thread that had just been
     * told to wait — two threads to do one thread's work.
     *
     * <p>The request object carries both shapes — to a file, or to a stream —
     * and choosing between them is a property of the request, not of the
     * caller.
     *
     * <p>Returns how the transfer ended rather than throwing when it ends
     * badly. Rejected, cancelled and failed are things a transfer does, not
     * exceptional control flow, and the queue above this decides what to do
     * about each. What still throws is a request that never became a transfer
     * at all: a bad argument, a client that is not logged in, a duplicate token
     * or a duplicate transfer.
     *
     * @param request the download to perform
     * @return how it ended
     */
    TransferOutcome download(DownloadRequest request) {
        Objects.requireNonNull(request, "request");
        return Transfers.outcomeOf(request.isToStream() ? downloadToStream(request) : downloadToFile(request));
    }

    /**
     * Uploads what the request describes.
     *
     * <p>The counterpart to {@link #download(DownloadRequest)}; see there.
     *
     * @param request the upload to perform
     * @return how it ended
     */
    TransferOutcome upload(UploadRequest request) {
        Objects.requireNonNull(request, "request");
        return Transfers.outcomeOf(request.isFromStream() ? uploadFromStream(request) : uploadFromFile(request));
    }

    /** Returns a local file's size, or zero when there is no file yet. */
    private long localFileSize(String path) {
        try {
            return io.getFileInfo(path).size();
        } catch (IOException missing) {
            return 0;
        }
    }

    /** Downloads to a local path, opening it as the destination stream. */
    private Transfer downloadToFile(DownloadRequest request) {
        String requestedUsername = request.getUsername();
        String remoteFilename = request.getRemoteFilename();
        String localFilename = request.getLocalFilename();
        long startOffset = request.getStartOffset();
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireText(remoteFilename, "remoteFilename");
        CommonUtils.requireText(localFilename, "localFilename");
        validateDownloadRange(request.getSize(), startOffset);
        server.requireLoggedIn("download files");
        int transferToken = request.getToken() == null ? tokens.nextToken() : request.getToken();
        validateDownloadUniqueness(requestedUsername, remoteFilename, transferToken);
        // A stream this opened is a stream this closes, whatever the request
        // said about a stream it did not open.
        TransferOptions options = (request.getOptions() == null ? new TransferOptions() : request.getOptions())
                .withDisposalOptions(null, true);
        return runDownload(
                requestedUsername,
                remoteFilename,
                () -> {
                    try {
                        if (startOffset > 0) {
                            // Appending ignores position: O_APPEND puts every
                            // write at end-of-file whatever a seek said. If
                            // the local file is not exactly startOffset bytes,
                            // the resumed bytes would land at the wrong place
                            // with no error. The C# source fails loudly on the
                            // same mismatch; so does this.
                            long existing = localFileSize(localFilename);
                            if (existing != startOffset) {
                                throw new IOException("Cannot resume " + localFilename
                                        + " from offset " + startOffset + ": the local file is "
                                        + existing + " bytes, and appending would put the"
                                        + " requested bytes at the wrong place silently");
                            }
                        }
                        return io.getOutputStream(localFilename, startOffset > 0);
                    } catch (IOException failure) {
                        throw new UncheckedIOException(failure);
                    }
                },
                request.getSize(),
                startOffset,
                transferToken,
                options,
                request.getOffer(),
                CommonUtils.token(request.getCancellationSignal()));
    }

    /** Downloads to a caller-supplied stream. */
    private Transfer downloadToStream(DownloadRequest request) {
        String requestedUsername = request.getUsername();
        String remoteFilename = request.getRemoteFilename();
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireText(remoteFilename, "remoteFilename");
        validateDownloadRange(request.getSize(), request.getStartOffset());
        Objects.requireNonNull(request.getOutputStreamFactory(), "outputStreamFactory");
        server.requireLoggedIn("download files");
        int transferToken = request.getToken() == null ? tokens.nextToken() : request.getToken();
        validateDownloadUniqueness(requestedUsername, remoteFilename, transferToken);
        return runDownload(
                requestedUsername,
                remoteFilename,
                request.getOutputStreamFactory(),
                request.getSize(),
                request.getStartOffset(),
                transferToken,
                request.getOptions() == null ? new TransferOptions() : request.getOptions(),
                request.getOffer(),
                CommonUtils.token(request.getCancellationSignal()));
    }

    /** Uploads a local path, opening it as the source stream. */
    private Transfer uploadFromFile(UploadRequest request) {
        String requestedUsername = request.getUsername();
        String remoteFilename = request.getRemoteFilename();
        String localFilename = request.getLocalFilename();
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireText(remoteFilename, "remoteFilename");
        CommonUtils.requireText(localFilename, "localFilename");
        if (!io.exists(localFilename)) {
            throw new UncheckedIOException(
                    new FileNotFoundException("The local file does not exist: " + localFilename));
        }
        server.requireLoggedIn("upload files");
        try (InputStream ignored = io.getInputStream(localFilename)) {
            // Probe readability before allocating a transfer token.
        } catch (IOException failure) {
            throw new UncheckedIOException(
                    "The local file " + localFilename + " could not be opened for reading: "
                            + Failures.message(failure),
                    failure);
        }

        int transferToken = request.getToken() == null ? tokens.nextToken() : request.getToken();
        validateUploadUniqueness(requestedUsername, remoteFilename, transferToken);
        TransferOptions fileOptions = (request.getOptions() == null ? new TransferOptions() : request.getOptions())
                .withDisposalOptions(true, null);
        long size;
        try {
            size = io.getFileInfo(localFilename).size();
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
        return runUpload(
                requestedUsername,
                remoteFilename,
                size,
                ignoredOffset -> {
                    try {
                        return io.getInputStream(localFilename);
                    } catch (IOException failure) {
                        throw new UncheckedIOException(failure);
                    }
                },
                transferToken,
                fileOptions,
                CommonUtils.token(request.getCancellationSignal()));
    }

    /** Uploads from a caller-supplied stream. */
    private Transfer uploadFromStream(UploadRequest request) {
        String requestedUsername = request.getUsername();
        String remoteFilename = request.getRemoteFilename();
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireText(remoteFilename, "remoteFilename");
        if (request.getSize() < 0) {
            throw new IllegalArgumentException("size must be greater than or equal to zero");
        }
        Objects.requireNonNull(request.getInputStreamFactory(), "inputStreamFactory");
        server.requireLoggedIn("upload files");
        int transferToken = request.getToken() == null ? tokens.nextToken() : request.getToken();
        validateUploadUniqueness(requestedUsername, remoteFilename, transferToken);
        return runUpload(
                requestedUsername,
                remoteFilename,
                request.getSize(),
                request.getInputStreamFactory(),
                transferToken,
                request.getOptions() == null ? new TransferOptions() : request.getOptions(),
                CommonUtils.token(request.getCancellationSignal()));
    }

    /**
     * Asks a peer where we are in its queue for a download already in flight,
     * uncancellably; the queue's periodic poll is the only caller.
     *
     * @param requestedUsername the peer
     * @param filename the file
     * @return the peer's place-in-queue
     */
    Integer getDownloadPlaceInQueue(String requestedUsername, String filename) {
        return getDownloadPlaceInQueue(requestedUsername, filename, CancellationSignal.none());
    }

    /**
     * Asks a peer where we are in its queue for a download already in flight.
     *
     * @param requestedUsername the peer
     * @param filename the file
     * @param cancellationSignal stops the request
     * @return the peer's place-in-queue
     */
    Integer getDownloadPlaceInQueue(String requestedUsername, String filename, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireText(filename, "filename");
        server.requireLoggedIn("check download queue position");
        if (matching(downloads, requestedUsername, filename).isEmpty()) {
            throw new TransferNotFoundException(
                    "A download of " + filename + " from user " + requestedUsername + " is not active");
        }
        CancellationSignal token = CommonUtils.token(cancellationSignal);
        try {
            Wait<PlaceInQueueResponse> responseWait = waiter.register(
                    new WaitKey(MessageCode.Peer.PLACE_IN_QUEUE_RESPONSE, requestedUsername, filename),
                    PlaceInQueueResponse.class,
                    null,
                    token);
            InetSocketAddress endpoint = endpoints.resolve(requestedUsername, token);
            MessageConnection connection = peers.get().getOrAddMessageConnection(requestedUsername, endpoint, token);
            connection.write(new PlaceInQueueRequest(filename), CommonUtils.token(token));
            return responseWait.await().getPlaceInQueue();
        } catch (Throwable failure) {
            throw Failures.raise(
                    failure,
                    "Failed to fetch place in queue for download of " + filename + " from " + requestedUsername + ": ",
                    UserOfflineException.class);
        }
    }

    // --- admitting a run ----------------------------------------------------

    private Transfer runDownload(
            String requestedUsername,
            String remoteFilename,
            Supplier<OutputStream> outputStreamFactory,
            Long size,
            long startOffset,
            int token,
            TransferOptions transferOptions,
            TransferRequest offer,
            CancellationSignal cancellationSignal) {
        TransferInternal download = new TransferInternal(
                TransferDirection.DOWNLOAD, requestedUsername, remoteFilename, token, transferOptions);
        download.setStartOffset(startOffset);
        download.setSize(size);
        String uniqueKey = downloadUniqueKey(requestedUsername, remoteFilename);

        if (uniqueKeys.putIfAbsent(uniqueKey, true) != null) {
            throw new DuplicateTransferException(
                    "Duplicate download of " + remoteFilename + " from " + requestedUsername + " aborted");
        }
        if (downloads.putIfAbsent(token, download) != null) {
            uniqueKeys.remove(uniqueKey);
            throw new DuplicateTransferException(
                    "Duplicate download of " + remoteFilename + " from " + requestedUsername + " aborted");
        }

        return new DownloadRun(
                        this, download, outputStreamFactory, transferOptions, offer, cancellationSignal, uniqueKey)
                .execute();
    }

    private Transfer runUpload(
            String requestedUsername,
            String remoteFilename,
            long size,
            LongFunction<InputStream> inputStreamFactory,
            int token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        TransferInternal upload = new TransferInternal(
                TransferDirection.UPLOAD, requestedUsername, remoteFilename, token, transferOptions);
        upload.setSize(size);
        String uniqueKey = uploadUniqueKey(requestedUsername, remoteFilename);

        if (uniqueKeys.putIfAbsent(uniqueKey, true) != null) {
            throw new DuplicateTransferException(
                    "Duplicate upload of " + remoteFilename + " to " + requestedUsername + " aborted");
        }
        if (uploads.putIfAbsent(token, upload) != null) {
            uniqueKeys.remove(uniqueKey);
            throw new DuplicateTransferException(
                    "Duplicate upload of " + remoteFilename + " to " + requestedUsername + " aborted");
        }

        return new UploadRun(this, upload, inputStreamFactory, transferOptions, cancellationSignal, uniqueKey)
                .execute();
    }

    // --- rules --------------------------------------------------------------

    static String downloadUniqueKey(String requestedUsername, String remoteFilename) {
        return "Download:" + requestedUsername + ":" + remoteFilename;
    }

    static String uploadUniqueKey(String requestedUsername, String remoteFilename) {
        return "Upload:" + requestedUsername + ":" + remoteFilename;
    }

    static void validateDownloadRange(Long size, long startOffset) {
        if (size != null && size < 0) {
            throw new IllegalArgumentException("size must be greater than or equal to zero");
        }
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must be greater than or equal to zero");
        }
        if (startOffset > 0 && size == null) {
            throw new NullPointerException("size must be specified when startOffset is non-zero");
        }
    }

    void validateDownloadUniqueness(String requestedUsername, String remoteFilename, int token) {
        if (uploads.containsKey(token) || downloads.containsKey(token)) {
            throw new DuplicateTokenException("The specified or generated token " + token + " is already in progress");
        }
        if (!matching(downloads, requestedUsername, remoteFilename).isEmpty()
                || uniqueKeys.containsKey(downloadUniqueKey(requestedUsername, remoteFilename))) {
            throw new DuplicateTransferException("An active or queued download of "
                    + remoteFilename + " from " + requestedUsername
                    + " is already in progress");
        }
    }

    void validateUploadUniqueness(String requestedUsername, String remoteFilename, int token) {
        if (uploads.containsKey(token) || downloads.containsKey(token)) {
            throw new DuplicateTokenException("The specified or generated token " + token + " is already in progress");
        }
        if (!matching(uploads, requestedUsername, remoteFilename).isEmpty()
                || uniqueKeys.containsKey(uploadUniqueKey(requestedUsername, remoteFilename))) {
            throw new DuplicateTransferException("An active or queued upload of "
                    + remoteFilename + " to " + requestedUsername
                    + " is already in progress");
        }
    }
}
