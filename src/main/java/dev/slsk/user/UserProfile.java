// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.user;

import java.util.Objects;
import java.util.Optional;

/**
 * What other users see when they ask about this account.
 *
 * <p>This replaces {@code UserInfoResolver}, a callback invoked on every peer
 * who asked. Every application implemented it the same way — return the same
 * value every time — because the answer does not depend on who is asking or on
 * when. A callback for a constant is a callback that exists only so the library
 * can pretend not to hold state.
 *
 * <p>The slot and queue figures are advertised rather than measured: they are
 * what this account claims about its willingness to upload, which is what a peer
 * uses to decide whether to bother queueing. The upload policy in 1.1 is what
 * makes them true.
 *
 * @param description the free text shown alongside the account
 * @param picture an avatar, if there is one
 * @param uploadSlots how many uploads this account will run at once
 * @param queueLength how many uploads are waiting
 * @param hasFreeUploadSlot whether one is free right now
 */
public record UserProfile(
        String description, Optional<byte[]> picture, int uploadSlots, int queueLength, boolean hasFreeUploadSlot) {

    /** Validates and returns the profile. */
    public UserProfile {
        Objects.requireNonNull(description, "description");
        Objects.requireNonNull(picture, "picture");
        if (uploadSlots < 0) {
            throw new IllegalArgumentException("uploadSlots must not be negative: " + uploadSlots);
        }
        if (queueLength < 0) {
            throw new IllegalArgumentException("queueLength must not be negative: " + queueLength);
        }
    }

    /**
     * Returns the profile of an account that says nothing about itself.
     *
     * <p>The default, and what a peer sees until something else is set. Empty
     * rather than absent, because a peer that asks has to be answered: silence
     * reads as a broken client, and clients that look broken do not get served.
     *
     * @return the empty profile
     */
    public static UserProfile empty() {
        return new UserProfile("", Optional.empty(), 0, 0, false);
    }

    /**
     * Returns a profile with a description and nothing else.
     *
     * @param description the free text shown alongside the account
     * @return the profile
     */
    public static UserProfile of(String description) {
        return new UserProfile(description, Optional.empty(), 0, 0, false);
    }
}
