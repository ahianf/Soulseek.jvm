// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.spi;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;

/**
 * Where the bytes of a download go.
 *
 * <p>This replaces {@code DownloadStreamFactory}, which handed the library an
 * {@code OutputStream} and left everything else to the caller: whether a partial
 * file was safe to publish, what "resume" meant, and who deleted what after a
 * failure. Those are not questions an application should have to answer to
 * download a file correctly, and every application that answered them answered
 * them differently.
 *
 * <p>The contract is three calls and one guarantee. {@link #open} is told where
 * to start, so a resumed transfer says so rather than the sink inferring it.
 * {@link #commit} runs exactly once, on success, and is where the result becomes
 * visible. {@link #discard} runs instead, on failure or cancellation, and must
 * not throw — it is called from a failure path that has already gone wrong once.
 *
 * <p>The guarantee is that nothing incomplete is ever visible at the
 * destination. {@link #file} keeps the partial bytes beside the destination and
 * renames atomically, so an interrupted download leaves a {@code .part} a retry
 * can resume from and never a truncated file that looks finished.
 */
public interface TransferSink {

    /**
     * Opens for writing.
     *
     * <p>A non-zero offset is a resume: the first byte written belongs at that
     * position, and everything already there below it is to be kept.
     *
     * @param resumeOffset where the incoming bytes start
     * @return the channel to write to
     * @throws IOException if the destination cannot be opened
     */
    WritableByteChannel open(long resumeOffset) throws IOException;

    /**
     * Makes the result visible. Called exactly once, on success.
     *
     * @throws IOException if the result cannot be published
     */
    void commit() throws IOException;

    /**
     * Abandons this attempt. Called on failure or cancellation, and must not
     * throw.
     */
    void discard();

    /**
     * A sink that writes beside the destination and renames atomically.
     *
     * <p>Bytes go to {@code <destination>.part}. On {@link #commit} that file is
     * moved onto the destination, atomically where the filesystem supports it,
     * so a reader sees either the old file or the whole new one. On {@link
     * #discard} the part file is left alone, because it is exactly what a retry
     * needs to resume from.
     *
     * @param destination where the finished file belongs
     * @return the sink
     */
    static TransferSink file(Path destination) {
        return new FileTransferSink(destination);
    }
}
