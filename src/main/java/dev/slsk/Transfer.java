// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * A snapshot of a single file transfer.
 */
public class Transfer {
    private final double averageSpeed;
    private final long bytesRemaining;
    private final long bytesTransferred;
    private final TransferDirection direction;
    private final Duration elapsedTime;
    private final Instant endTime;
    private final Throwable exception;
    private final String filename;
    private final InetSocketAddress ipEndPoint;
    private final double percentComplete;
    private final Duration remainingTime;
    private final Integer remoteToken;
    private final long size;
    private final long startOffset;
    private final Instant startTime;
    private final TransferStates state;
    private final int token;
    private final String username;

    /**
     * Creates a transfer with default progress and connection data.
     *
     * @param direction the transfer direction
     * @param username the peer username
     * @param filename the transferred filename
     * @param token the local transfer token
     * @param state the transfer state
     * @param size the file size in bytes
     * @param startOffset the starting offset in bytes
     */
    public Transfer(
            TransferDirection direction,
            String username,
            String filename,
            int token,
            TransferStates state,
            long size,
            long startOffset) {
        this(direction, username, filename, token, state, size, startOffset, 0, 0, null, null, null, null, null);
    }

    /**
     * Creates a transfer with a transferred-byte count.
     *
     * @param direction the transfer direction
     * @param username the peer username
     * @param filename the transferred filename
     * @param token the local transfer token
     * @param state the transfer state
     * @param size the file size in bytes
     * @param startOffset the starting offset in bytes
     * @param bytesTransferred the transferred-byte count
     */
    public Transfer(
            TransferDirection direction,
            String username,
            String filename,
            int token,
            TransferStates state,
            long size,
            long startOffset,
            long bytesTransferred) {
        this(
                direction,
                username,
                filename,
                token,
                state,
                size,
                startOffset,
                bytesTransferred,
                0,
                null,
                null,
                null,
                null,
                null);
    }

    /**
     * Creates a transfer with progress data.
     *
     * @param direction the transfer direction
     * @param username the peer username
     * @param filename the transferred filename
     * @param token the local transfer token
     * @param state the transfer state
     * @param size the file size in bytes
     * @param startOffset the starting offset in bytes
     * @param bytesTransferred the transferred-byte count
     * @param averageSpeed the average speed in bytes per second
     */
    public Transfer(
            TransferDirection direction,
            String username,
            String filename,
            int token,
            TransferStates state,
            long size,
            long startOffset,
            long bytesTransferred,
            double averageSpeed) {
        this(
                direction,
                username,
                filename,
                token,
                state,
                size,
                startOffset,
                bytesTransferred,
                averageSpeed,
                null,
                null,
                null,
                null,
                null);
    }

    /**
     * Creates a transfer with a start time.
     *
     * @param direction the transfer direction
     * @param username the peer username
     * @param filename the transferred filename
     * @param token the local transfer token
     * @param state the transfer state
     * @param size the file size in bytes
     * @param startOffset the starting offset in bytes
     * @param bytesTransferred the transferred-byte count
     * @param averageSpeed the average speed in bytes per second
     * @param startTime the UTC transfer start time
     */
    public Transfer(
            TransferDirection direction,
            String username,
            String filename,
            int token,
            TransferStates state,
            long size,
            long startOffset,
            long bytesTransferred,
            double averageSpeed,
            Instant startTime) {
        this(
                direction,
                username,
                filename,
                token,
                state,
                size,
                startOffset,
                bytesTransferred,
                averageSpeed,
                startTime,
                null,
                null,
                null,
                null);
    }

    /**
     * Creates a transfer with start and end times.
     *
     * @param direction the transfer direction
     * @param username the peer username
     * @param filename the transferred filename
     * @param token the local transfer token
     * @param state the transfer state
     * @param size the file size in bytes
     * @param startOffset the starting offset in bytes
     * @param bytesTransferred the transferred-byte count
     * @param averageSpeed the average speed in bytes per second
     * @param startTime the UTC transfer start time
     * @param endTime the UTC transfer end time
     */
    public Transfer(
            TransferDirection direction,
            String username,
            String filename,
            int token,
            TransferStates state,
            long size,
            long startOffset,
            long bytesTransferred,
            double averageSpeed,
            Instant startTime,
            Instant endTime) {
        this(
                direction,
                username,
                filename,
                token,
                state,
                size,
                startOffset,
                bytesTransferred,
                averageSpeed,
                startTime,
                endTime,
                null,
                null,
                null);
    }

    /**
     * Creates a transfer with a remote token.
     *
     * @param direction the transfer direction
     * @param username the peer username
     * @param filename the transferred filename
     * @param token the local transfer token
     * @param state the transfer state
     * @param size the file size in bytes
     * @param startOffset the starting offset in bytes
     * @param bytesTransferred the transferred-byte count
     * @param averageSpeed the average speed in bytes per second
     * @param startTime the UTC transfer start time
     * @param endTime the UTC transfer end time
     * @param remoteToken the peer's transfer token
     */
    public Transfer(
            TransferDirection direction,
            String username,
            String filename,
            int token,
            TransferStates state,
            long size,
            long startOffset,
            long bytesTransferred,
            double averageSpeed,
            Instant startTime,
            Instant endTime,
            Integer remoteToken) {
        this(
                direction,
                username,
                filename,
                token,
                state,
                size,
                startOffset,
                bytesTransferred,
                averageSpeed,
                startTime,
                endTime,
                remoteToken,
                null,
                null);
    }

    /**
     * Creates a transfer with remote connection data.
     *
     * @param direction the transfer direction
     * @param username the peer username
     * @param filename the transferred filename
     * @param token the local transfer token
     * @param state the transfer state
     * @param size the file size in bytes
     * @param startOffset the starting offset in bytes
     * @param bytesTransferred the transferred-byte count
     * @param averageSpeed the average speed in bytes per second
     * @param startTime the UTC transfer start time
     * @param endTime the UTC transfer end time
     * @param remoteToken the peer's transfer token
     * @param ipEndPoint the remote connection endpoint
     */
    public Transfer(
            TransferDirection direction,
            String username,
            String filename,
            int token,
            TransferStates state,
            long size,
            long startOffset,
            long bytesTransferred,
            double averageSpeed,
            Instant startTime,
            Instant endTime,
            Integer remoteToken,
            InetSocketAddress ipEndPoint) {
        this(
                direction,
                username,
                filename,
                token,
                state,
                size,
                startOffset,
                bytesTransferred,
                averageSpeed,
                startTime,
                endTime,
                remoteToken,
                ipEndPoint,
                null);
    }

    /**
     * Creates a transfer.
     *
     * @param direction the transfer direction
     * @param username the peer username
     * @param filename the transferred filename
     * @param token the local transfer token
     * @param state the transfer state
     * @param size the file size in bytes
     * @param startOffset the starting offset in bytes
     * @param bytesTransferred the transferred-byte count
     * @param averageSpeed the average speed in bytes per second
     * @param startTime the UTC transfer start time
     * @param endTime the UTC transfer end time
     * @param remoteToken the peer's transfer token
     * @param ipEndPoint the remote connection endpoint
     * @param exception the failure exception
     */
    public Transfer(
            TransferDirection direction,
            String username,
            String filename,
            int token,
            TransferStates state,
            long size,
            long startOffset,
            long bytesTransferred,
            double averageSpeed,
            Instant startTime,
            Instant endTime,
            Integer remoteToken,
            InetSocketAddress ipEndPoint,
            Throwable exception) {
        this.direction = Objects.requireNonNull(direction, "direction");
        this.username = username;
        this.filename = filename;
        this.token = token;
        this.state = Objects.requireNonNull(state, "state");
        this.size = size;
        this.startOffset = startOffset;
        this.bytesTransferred = bytesTransferred;
        this.averageSpeed = averageSpeed;
        this.startTime = startTime;
        this.endTime = endTime;
        this.remoteToken = remoteToken;
        this.ipEndPoint = ipEndPoint;
        this.exception = exception;

        bytesRemaining = size - bytesTransferred;
        elapsedTime = startTime == null ? null : Duration.between(startTime, endTime == null ? Instant.now() : endTime);
        percentComplete = size == 0 ? 0 : (bytesTransferred / (double) size) * 100;
        remainingTime = averageSpeed == 0 ? null : durationFromSeconds(bytesRemaining / averageSpeed);
    }

    /**
     * Returns the current average transfer speed.
     *
     * @return the average speed
     */
    public final double getAverageSpeed() {
        return averageSpeed;
    }

    /**
     * Returns the remaining-byte count.
     *
     * @return the remaining-byte count
     */
    public final long getBytesRemaining() {
        return bytesRemaining;
    }

    /**
     * Returns the transferred-byte count.
     *
     * @return the transferred-byte count
     */
    public final long getBytesTransferred() {
        return bytesTransferred;
    }

    /**
     * Returns the transfer direction.
     *
     * @return the transfer direction
     */
    public final TransferDirection getDirection() {
        return direction;
    }

    /**
     * Returns the elapsed duration, if the transfer has started.
     *
     * @return the elapsed duration, or {@code null}
     */
    public final Duration getElapsedTime() {
        return elapsedTime;
    }

    /**
     * Returns the UTC transfer end time.
     *
     * @return the end time, or {@code null}
     */
    public final Instant getEndTime() {
        return endTime;
    }

    /**
     * Returns the failure exception.
     *
     * @return the failure exception, or {@code null}
     */
    public final Throwable getException() {
        return exception;
    }

    /**
     * Returns the transferred filename.
     *
     * @return the filename
     */
    public final String getFilename() {
        return filename;
    }

    /**
     * Returns the remote connection endpoint.
     *
     * @return the endpoint, or {@code null}
     */
    public final InetSocketAddress getIpEndPoint() {
        return ipEndPoint;
    }

    /**
     * Returns the progress percentage.
     *
     * @return the progress percentage
     */
    public final double getPercentComplete() {
        return percentComplete;
    }

    /**
     * Returns the projected remaining duration.
     *
     * @return the remaining duration, or {@code null}
     */
    public final Duration getRemainingTime() {
        return remainingTime;
    }

    /**
     * Returns the remote transfer token.
     *
     * @return the remote token, or {@code null}
     */
    public final Integer getRemoteToken() {
        return remoteToken;
    }

    /**
     * Returns the file size.
     *
     * @return the file size in bytes
     */
    public final long getSize() {
        return size;
    }

    /**
     * Returns the starting offset.
     *
     * @return the starting offset in bytes
     */
    public final long getStartOffset() {
        return startOffset;
    }

    /**
     * Returns the UTC transfer start time.
     *
     * @return the start time, or {@code null}
     */
    public final Instant getStartTime() {
        return startTime;
    }

    /**
     * Returns the transfer state.
     *
     * @return the transfer state
     */
    public final TransferStates getState() {
        return state;
    }

    /**
     * Returns the local transfer token.
     *
     * @return the local token
     */
    public final int getToken() {
        return token;
    }

    /**
     * Returns the peer username.
     *
     * @return the peer username
     */
    public final String getUsername() {
        return username;
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
        long secondsPart = truncatedTicks / 10_000_000;
        long nanosecondsPart = (truncatedTicks % 10_000_000) * 100;
        return Duration.ofSeconds(secondsPart, nanosecondsPart);
    }
}
