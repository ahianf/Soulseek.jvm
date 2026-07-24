// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.transfer;

import dev.slsk.Transfer;
import dev.slsk.TransferDirection;
import dev.slsk.TransferStates;
import dev.slsk.common.Constants;
import dev.slsk.common.WaitKey;
import dev.slsk.network.tcp.IConnection;
import dev.slsk.options.TransferOptions;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** The mutable internal state of a single file transfer. */
public final class TransferInternal {
    private static final double SPEED_ALPHA = 2f / 10;

    private double averageSpeed;
    private long bytesTransferred;
    private final Clock clock;
    private IConnection connection;
    private final TransferDirection direction;
    private Instant endTime;
    private Throwable exception;
    private final String filename;
    private double lastProgressBytes;
    private Instant lastProgressTime;
    private final TransferOptions options;
    private final int progressUpdateLimit;
    private Integer remoteToken;
    private final CompletableFuture<Boolean> remoteTaskCompletionSource = new CompletableFuture<>();
    private Long size;
    private boolean speedInitialized;
    private long startOffset;
    private Instant startTime;
    private TransferStates state = TransferStates.NONE;
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
    public synchronized IConnection getConnection() {
        return connection;
    }

    /** Sets the transfer connection. */
    public synchronized void setConnection(IConnection value) {
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
    public synchronized InetSocketAddress getIpEndPoint() {
        return connection == null ? null : connection.getIpEndPoint();
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

    /** Returns the remote-disposition completion future. */
    public CompletableFuture<Boolean> getRemoteTaskCompletionSource() {
        return remoteTaskCompletionSource;
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

    /** Returns the transfer state. */
    public synchronized TransferStates getState() {
        return state;
    }

    /** Sets the transfer state and records transition timestamps. */
    public synchronized void setState(TransferStates value) {
        Objects.requireNonNull(value, "value");
        Instant time = clock.instant();
        if (value.hasFlag(TransferStates.IN_PROGRESS) && startTime == null) {
            startTime = time;
        } else if (value.hasFlag(TransferStates.COMPLETED) && endTime == null) {
            endTime = time;
            if (startTime == null) {
                startTime = time;
            }
        }

        state = value;
        if (state.hasFlag(TransferStates.COMPLETED)) {
            updateProgress(bytesTransferred);
        }
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
        if (state.hasFlag(TransferStates.COMPLETED)) {
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
                state,
                size == null ? 0 : size,
                startOffset,
                bytesTransferred,
                averageSpeed,
                startTime,
                endTime,
                remoteToken,
                getIpEndPoint(),
                exception);
    }

    private static double durationSeconds(Instant start, Instant end) {
        double milliseconds = Duration.between(start, end).toNanos() / 1_000_000d;
        return Math.max(1, milliseconds) / 1_000d;
    }

    private static Duration durationFromSeconds(double seconds) {
        if (Double.isNaN(seconds)) {
            throw new IllegalArgumentException("TimeSpan does not accept NaN");
        }
        double ticks = seconds * 10_000_000;
        if (!Double.isFinite(ticks) || ticks >= 0x1.0p63 || ticks < -0x1.0p63) {
            throw new ArithmeticException("TimeSpan overflowed because the duration is too long");
        }
        long truncatedTicks = (long) ticks;
        return Duration.ofSeconds(truncatedTicks / 10_000_000, (truncatedTicks % 10_000_000) * 100);
    }
}
