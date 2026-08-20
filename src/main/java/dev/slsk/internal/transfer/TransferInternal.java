// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.Settlement;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.network.tcp.Connection;
import dev.slsk.internal.options.TransferOptions;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** The mutable internal state of a single file transfer. */
public final class TransferInternal {
    private static final double SPEED_ALPHA = 2f / 10;

    private double averageSpeed;
    private long bytesTransferred;
    private final Clock clock;
    private Connection connection;
    private final TransferDirection direction;
    private Instant endTime;
    private Throwable exception;
    private final String filename;
    private double lastProgressBytes;
    private Instant lastProgressTime;
    private final TransferOptions options;
    private final int progressUpdateLimit;
    private Integer remoteToken;
    private final Settlement<Void> settlement = new Settlement<>();
    private Long size;
    private boolean speedInitialized;
    private long startOffset;
    private Instant startTime;
    private TransferPhase phase = TransferPhase.NONE;
    private TransferQueueLocation queueLocation;
    private TransferTermination termination;
    private final int token;
    private final String username;
    private final WaitKey waitKey;

    /** Creates a transfer using default options. */
    public TransferInternal(TransferDirection direction, String username, String filename, int token) {
        this(direction, username, filename, token, null);
    }

    /** Creates a transfer. */
    public TransferInternal(
            TransferDirection direction, String username, String filename, int token, TransferOptions options) {
        this(direction, username, filename, token, options, Clock.systemUTC(), 1_000);
    }

    TransferInternal(
            TransferDirection direction,
            String username,
            String filename,
            int token,
            TransferOptions options,
            Clock clock,
            int progressUpdateLimit) {
        this.direction = Objects.requireNonNull(direction, "direction");
        this.username = username;
        this.filename = filename;
        this.token = token;
        this.options = options == null ? new TransferOptions() : options;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.progressUpdateLimit = progressUpdateLimit;
        waitKey = new WaitKey(Constants.WaitKey.TRANSFER, direction, username, filename, token);
    }

    /** Returns the average transfer speed in bytes per second. */
    public synchronized double getAverageSpeed() {
        return averageSpeed;
    }

    /** Returns the remaining byte count. */
    public synchronized long getBytesRemaining() {
        return (size == null ? 0 : size) - bytesTransferred;
    }

    /** Returns the transferred byte count. */
    public synchronized long getBytesTransferred() {
        return bytesTransferred;
    }

    /** Returns the transfer connection, or {@code null}. */
    public synchronized Connection getConnection() {
        return connection;
    }

    /** Sets the transfer connection. */
    public synchronized void setConnection(Connection value) {
        connection = value;
    }

    /** Returns the transfer direction. */
    public TransferDirection getDirection() {
        return direction;
    }

    /** Returns elapsed transfer time, or {@code null}. */
    public synchronized Duration getElapsedTime() {
        return startTime == null ? null : Duration.between(startTime, endTime == null ? clock.instant() : endTime);
    }

    /** Returns the completion time, or {@code null}. */
    public synchronized Instant getEndTime() {
        return endTime;
    }

    /** Returns the failure exception, or {@code null}. */
    public synchronized Throwable getException() {
        return exception;
    }

    /** Sets the failure exception. */
    public synchronized void setException(Throwable value) {
        exception = value;
    }

    /** Returns the filename. */
    public String getFilename() {
        return filename;
    }

    /** Returns the transfer endpoint, or {@code null}. */
    public synchronized InetSocketAddress getIpEndpoint() {
        return connection == null ? null : connection.getIpEndpoint();
    }

    /** Returns the transfer options. */
    public TransferOptions getOptions() {
        return options;
    }

    /** Returns transfer completion as a percentage. */
    public synchronized double getPercentComplete() {
        return size == null ? 0 : (bytesTransferred / (double) size) * 100;
    }

    /** Returns the projected remaining duration, or {@code null}. */
    public synchronized Duration getRemainingTime() {
        return averageSpeed == 0 ? null : durationFromSeconds(getBytesRemaining() / averageSpeed);
    }

    /** Returns the remote token, or {@code null}. */
    public synchronized Integer getRemoteToken() {
        return remoteToken;
    }

    /** Sets the remote token. */
    public synchronized void setRemoteToken(Integer value) {
        remoteToken = value;
    }

    /**
     * Returns how this transfer ended, or the cell that will say so.
     *
     * <p>Settled by whichever of the three racing parties gets there first: the
     * transfer's own thread when the bytes stop moving, the connection's
     * disconnect callback, or a peer read loop delivering {@code UploadFailed}
     * or {@code UploadDenied}. See {@link Settlement}.
     *
     * @return the settlement, never {@code null}
     */
    public Settlement<Void> settlement() {
        return settlement;
    }

    /** Returns the nullable transfer size. */
    public synchronized Long getSize() {
        return size;
    }

    /** Sets the nullable transfer size. */
    public synchronized void setSize(Long value) {
        size = value;
    }

    /** Returns the transfer start offset. */
    public synchronized long getStartOffset() {
        return startOffset;
    }

    /**
     * Sets the transfer start offset and fast-forwards progress bookkeeping.
     */
    public synchronized void setStartOffset(long value) {
        startOffset = value;
        bytesTransferred = value;
        lastProgressBytes = value;
    }

    /** Returns the transfer start time, or {@code null}. */
    public synchronized Instant getStartTime() {
        return startTime;
    }

    /** Returns the transfer phase. */
    public synchronized TransferPhase getPhase() {
        return phase;
    }

    /** Returns the queue location, or {@code null} outside the queued phase. */
    public synchronized TransferQueueLocation getQueueLocation() {
        return queueLocation;
    }

    /** Returns why the transfer ended, or {@code null} while it is active. */
    public synchronized TransferTermination getTermination() {
        return termination;
    }

    /** Sets a non-queued, non-terminal transfer phase. */
    public synchronized void setPhase(TransferPhase value) {
        Objects.requireNonNull(value, "value");
        if (value == TransferPhase.QUEUED || value == TransferPhase.COMPLETED) {
            throw new IllegalArgumentException("queued and completed phases require their detail value");
        }
        transition(value, null, null);
    }

    /** Moves the transfer into the queued phase at the specified side. */
    public synchronized void queue(TransferQueueLocation location) {
        transition(TransferPhase.QUEUED, Objects.requireNonNull(location, "location"), null);
    }

    /** Completes the transfer with the specified reason. */
    public synchronized void complete(TransferTermination reason) {
        transition(TransferPhase.COMPLETED, null, Objects.requireNonNull(reason, "reason"));
    }

    /** Returns the local transfer token. */
    public int getToken() {
        return token;
    }

    /** Returns the peer username. */
    public String getUsername() {
        return username;
    }

    /** Returns the transfer correlation key. */
    public WaitKey getWaitKey() {
        return waitKey;
    }

    /** Updates transferred bytes and speed estimates. */
    public synchronized void updateProgress(long value) {
        bytesTransferred = value;
        if (startTime == null) {
            return;
        }

        Instant now = clock.instant();
        if (phase == TransferPhase.COMPLETED) {
            averageSpeed = (bytesTransferred - startOffset) / durationSeconds(startTime, endTime);
            return;
        }

        if (size != null && bytesTransferred >= size) {
            averageSpeed = (bytesTransferred - startOffset) / durationSeconds(startTime, now);
            return;
        }

        Instant prior = lastProgressTime == null ? startTime : lastProgressTime;
        double elapsedMilliseconds = Duration.between(prior, now).toNanos() / 1_000_000d;
        if (elapsedMilliseconds >= progressUpdateLimit) {
            double currentSpeed = (bytesTransferred - lastProgressBytes) / (elapsedMilliseconds / 1_000d);
            averageSpeed =
                    !speedInitialized ? currentSpeed : ((currentSpeed - averageSpeed) * SPEED_ALPHA) + averageSpeed;
            speedInitialized = true;
            lastProgressTime = now;
            lastProgressBytes = bytesTransferred;
        }
    }

    /** Creates the public immutable snapshot of this transfer. */
    public synchronized Transfer toTransfer() {
        return new Transfer(
                direction,
                username,
                filename,
                token,
                phase,
                queueLocation,
                termination,
                size == null ? 0 : size,
                startOffset,
                bytesTransferred,
                averageSpeed,
                startTime,
                endTime,
                remoteToken,
                getIpEndpoint(),
                exception);
    }

    private void transition(
            TransferPhase newPhase, TransferQueueLocation newQueueLocation, TransferTermination newTermination) {
        Instant time = clock.instant();
        if (newPhase == TransferPhase.IN_PROGRESS && startTime == null) {
            startTime = time;
        } else if (newPhase == TransferPhase.COMPLETED && endTime == null) {
            endTime = time;
            if (startTime == null) {
                startTime = time;
            }
        }

        phase = newPhase;
        queueLocation = newQueueLocation;
        termination = newTermination;
        if (phase == TransferPhase.COMPLETED) {
            updateProgress(bytesTransferred);
        }
    }

    private static double durationSeconds(Instant start, Instant end) {
        double milliseconds = Duration.between(start, end).toNanos() / 1_000_000d;
        return Math.max(1, milliseconds) / 1_000d;
    }

    private static Duration durationFromSeconds(double seconds) {
        return Transfer.durationFromSeconds(seconds);
    }
}
