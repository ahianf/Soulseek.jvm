// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import dev.slsk.internal.SearchResponse;
import dev.slsk.search.SearchFile;
import java.util.ArrayList;
import java.util.List;

/**
 * Between a {@link dev.slsk.spi.ShareCatalog} and the wire.
 *
 * <p>Two translations, both dull and both necessary. The catalog speaks the
 * public vocabulary — {@code SearchFile}, {@code Directory} — and the message
 * codecs speak the wire's, which carries attribute codes and per-file
 * extensions. Nothing here decides anything; the interesting question was which
 * types the SPI should speak, and it is answered in {@code ShareCatalog}.
 *
 * <p>There was an {@code ask} here that ran a blocking catalog call on a virtual
 * thread, because its callers were read loops. The dispatch moved out to those
 * callers, where it covers the connect and the write the catalog answer is for
 * as well as the catalog itself.
 */
public final class Catalogs {

    /**
     * The most matches any search answer carries, on either answer path.
     *
     * <p>The wire has no limit; the reference clients stop well short of one.
     * A response of ten thousand files is not read by the peer that receives
     * it — it is discarded for being implausible — and sending it costs us the
     * bandwidth we would rather spend uploading.
     */
    public static final int MAXIMUM_SEARCH_MATCHES = 250;

    private Catalogs() {}

    /**
     * Converts a public file to the wire's shape.
     *
     * @param file the file
     * @return the wire file
     */
    public static File file(SearchFile file) {
        return new File(1, file.path(), file.size(), file.extension(), List.of());
    }

    /**
     * Converts a public directory to the wire's shape.
     *
     * @param directory the directory
     * @return the wire directory
     */
    public static Directory directory(dev.slsk.share.Directory directory) {
        List<File> files = new ArrayList<>(directory.files().size());
        for (SearchFile file : directory.files()) {
            files.add(file(file));
        }
        return new Directory(directory.name(), files);
    }

    /**
     * Converts a list of public directories to the wire's shape.
     *
     * @param directories the directories
     * @return the wire directories
     */
    public static List<Directory> directories(List<dev.slsk.share.Directory> directories) {
        List<Directory> converted = new ArrayList<>(directories.size());
        for (dev.slsk.share.Directory directory : directories) {
            converted.add(directory(directory));
        }
        return converted;
    }

    /**
     * Converts a browse response to the wire's shape.
     *
     * @param response the response
     * @return the wire response
     */
    public static BrowseResponse browse(dev.slsk.share.BrowseResponse response) {
        return new BrowseResponse(directories(response.directories()), directories(response.lockedDirectories()));
    }

    /**
     * Builds the search response we send a peer.
     *
     * @param username our own username, which the peer keys the response on
     * @param token the peer's search token
     * @param matches what the catalog matched
     * @param hasFreeUploadSlot whether we can serve right now
     * @param uploadSpeed our advertised speed
     * @param queueLength how many uploads are waiting
     * @return the wire response
     */
    public static SearchResponse searchResponse(
            String username,
            int token,
            List<SearchFile> matches,
            boolean hasFreeUploadSlot,
            int uploadSpeed,
            int queueLength) {
        List<File> files = new ArrayList<>(matches.size());
        for (SearchFile match : matches) {
            files.add(file(match));
        }
        return new SearchResponse(username, token, hasFreeUploadSlot, uploadSpeed, queueLength, files);
    }
}
