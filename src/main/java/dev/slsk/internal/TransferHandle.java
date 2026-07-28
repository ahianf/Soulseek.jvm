// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

/**
 * A transfer that the remote peer has accepted but not yet finished.
 *
 * <p>Enqueueing is genuinely two steps: the peer first accepts the request,
 * then the bytes move. The port expressed that as
 * {@code CompletableFuture<CompletableFuture<Transfer>>} — the outer future for
 * acceptance, the inner for completion — which is accurate and close to
 * unreadable.
 *
 * <p>The blocking API splits it in the obvious way. {@code enqueueDownload}
 * returns once the peer has accepted, handing back one of these; {@link #await}
 * blocks until the transfer finishes. A caller who wants the two phases on
 * different threads starts a virtual thread, which is theirs to decide rather
 * than the library's.
 */
public final class TransferHandle {

    private final CompletableFuture<Transfer> completion;

    TransferHandle(CompletableFuture<Transfer> completion) {
        this.completion = Objects.requireNonNull(completion, "completion");
    }

    /**
     * Blocks until the transfer finishes.
     *
     * @return the completed transfer
     * @throws SoulseekClientException if the transfer failed
     * @throws NoResponseException if the transfer timed out
     * @throws java.util.concurrent.CancellationException if it was cancelled
     */
    public Transfer await() {
        try {
            return completion.join();
        } catch (Throwable failure) {
            throw translate(failure);
        }
    }

    /** Returns whether the transfer has finished, one way or another. */
    public boolean isDone() {
        return completion.isDone();
    }

    /**
     * Translates the same way the client's blocking wrappers do, so a handle
     * and a direct call fail identically. See D11 in
     * {@code docs/fork-divergence.md} for why a timeout becomes
     * {@link NoResponseException}.
     */
    private static RuntimeException translate(Throwable failure) {
        Throwable cause = failure;
        while ((cause instanceof CompletionException || cause instanceof java.util.concurrent.ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof TimeoutException) {
            return new NoResponseException(cause.getMessage(), cause);
        }
        if (cause instanceof RuntimeException runtime) {
            return runtime;
        }
        return new SoulseekClientException(cause.getMessage(), cause);
    }
}
