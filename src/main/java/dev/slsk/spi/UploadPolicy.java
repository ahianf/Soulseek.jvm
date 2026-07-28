// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.spi;

import dev.slsk.RejectionReason;
import java.util.Objects;

/**
 * Who we serve, and in what order.
 *
 * <p>This replaces four callbacks that each answered part of the question and
 * none of which could see the others: {@code EnqueueDownloadCallback} said
 * whether to accept, {@code PlaceInQueueResolver} said where in the queue,
 * {@code TransferSlotAwaiter} decided when a slot was free, and {@code
 * TransferGovernor} metered the bytes. An application implementing four
 * disconnected functions cannot express "two slots, one per user, privileged
 * first" without keeping its own shadow state, and every application that tried
 * kept a different one.
 *
 * <p>One decision, from one request and one context. A policy that is a pure
 * function of its arguments can be tested without a client and reasoned about
 * without a diagram, which is the whole reason for the shape.
 */
@FunctionalInterface
public interface UploadPolicy {

    /**
     * Decides what to do about a peer's request.
     *
     * @param request who wants what
     * @param context what is true right now
     * @return the decision
     */
    Decision decide(UploadRequest request, UploadContext context);

    /** What a policy can say. */
    sealed interface Decision {

        /** Serve it now. */
        record Allow() implements Decision {}

        /**
         * Serve it later, and tell the peer where they are.
         *
         * @param position their place in the queue, counting from one
         */
        record Queue(int position) implements Decision {

            /** Validates and returns the decision. */
            public Queue {
                if (position < 1) {
                    throw new IllegalArgumentException("position counts from one: " + position);
                }
            }
        }

        /**
         * Refuse it, with a reason the peer's client can act on.
         *
         * @param reason why, classified
         * @param message why, as the peer will see it
         */
        record Deny(RejectionReason reason, String message) implements Decision {

            /** Validates and returns the decision. */
            public Deny {
                Objects.requireNonNull(reason, "reason");
                Objects.requireNonNull(message, "message");
            }
        }
    }

    /**
     * The default: slots, a per-user cap, and privileged users first.
     *
     * <p>Privileged precedence is here rather than in an application because it
     * is protocol-mandated: a user who has paid for privileges expects to jump
     * queues, and a client that ignores that is a client people stop downloading
     * from. Expressing it costs an application nothing here and a shadow queue
     * everywhere else.
     *
     * @param slots how many uploads run at once
     * @param perUser how many run at once for any one peer
     * @return the policy
     */
    static UploadPolicy standard(int slots, int perUser) {
        if (slots < 1) {
            throw new IllegalArgumentException("slots must be at least 1: " + slots);
        }
        if (perUser < 1) {
            throw new IllegalArgumentException("perUser must be at least 1: " + perUser);
        }
        return (request, context) -> {
            if (context.activeSlotsForRequester() >= perUser) {
                // Their own doing, so they are queued rather than refused: the
                // file is available, they simply already have our attention.
                return new Decision.Queue(context.queueDepth() + 1);
            }
            if (context.activeSlots() < slots) {
                return new Decision.Allow();
            }
            // Privileged users go to the front. Nothing here reorders anyone
            // already running; it decides where the next one waits.
            return new Decision.Queue(context.requesterIsPrivileged() ? 1 : context.queueDepth() + 1);
        };
    }

    /**
     * A policy that refuses everything.
     *
     * <p>What a client that shares nothing should say, and it says it as a
     * decision rather than by failing: a peer asking for a file we do not serve
     * gets an answer their client understands.
     *
     * @return the policy
     */
    static UploadPolicy refuseAll() {
        return (request, context) -> new Decision.Deny(RejectionReason.FILE_NOT_SHARED, "File not shared.");
    }
}
