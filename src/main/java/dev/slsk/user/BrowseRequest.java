// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.user;

import dev.slsk.CancellationSignal;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * A request to read what one user is sharing.
 *
 * <p>A record rather than four overloads, because a browse has four things
 * worth saying about it and only the first is required. The old surface
 * expressed the same options as {@code browse(user)}, {@code browse(user,
 * options)}, {@code browse(user, signal)} and {@code browse(user, options,
 * signal)} — the progressive-overload shape a C# optional parameter turns into,
 * which stops scaling at the fifth thing worth configuring.
 *
 * @param user whose share to read
 * @param timeout how long to wait for the peer to start responding
 * @param onProgress called as the response arrives, if anyone is watching
 * @param signal cancels the browse
 */
public record BrowseRequest(
        Username user, Duration timeout, Optional<Consumer<BrowseProgress>> onProgress, CancellationSignal signal) {

    /** The default wait for a peer to begin responding. */
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);

    /** Validates and returns the request. */
    public BrowseRequest {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(onProgress, "onProgress");
        Objects.requireNonNull(signal, "signal");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive: " + timeout);
        }
    }

    /**
     * Returns a request with defaults for everything but the user.
     *
     * @param user whose share to read
     * @return the request
     */
    public static BrowseRequest of(Username user) {
        return new BrowseRequest(user, DEFAULT_TIMEOUT, Optional.empty(), CancellationSignal.none());
    }

    /**
     * Returns this request with a different timeout.
     *
     * @param value how long to wait for the peer to start responding
     * @return the request
     */
    public BrowseRequest timeout(Duration value) {
        return new BrowseRequest(user, value, onProgress, signal);
    }

    /**
     * Returns this request with a progress callback.
     *
     * @param value called as the response arrives
     * @return the request
     */
    public BrowseRequest onProgress(Consumer<BrowseProgress> value) {
        return new BrowseRequest(user, timeout, Optional.of(value), signal);
    }

    /**
     * Returns this request with a cancellation signal.
     *
     * @param value cancels the browse
     * @return the request
     */
    public BrowseRequest cancelledBy(CancellationSignal value) {
        return new BrowseRequest(user, timeout, onProgress, value);
    }
}
