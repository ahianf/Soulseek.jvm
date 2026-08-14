// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.spi;

import dev.slsk.BrowseResponse;
import dev.slsk.Directory;
import dev.slsk.ShareIndex;
import dev.slsk.search.SearchFile;
import dev.slsk.user.Username;
import java.util.List;
import java.util.Optional;

/**
 * What this account is sharing, from the point of view of a peer asking.
 *
 * <p>Four callbacks used to ask this, one per question, each configured
 * separately and each free to disagree with the others: a browse could list a
 * file that a search would not match and an upload would refuse to open.
 * Nothing held them together, and nothing could, because they were four
 * unrelated function objects on an options record.
 *
 * <p>They are one interface here because they are one thing — a view of a share
 * — and because the interesting cases are exactly the ones where the answers
 * must agree. Every method takes the requester, because a share is allowed to
 * differ per peer: that is what a private share is.
 *
 * <p>Blocking, like every SPI here. It is called from a peer's connection, not
 * from a read loop, so a slow catalog delays one peer.
 */
public interface ShareCatalog {

    /**
     * Returns everything the requester may see.
     *
     * @param requester the peer who asked
     * @return the browse response
     */
    BrowseResponse browse(Username requester);

    /**
     * Returns the contents of one directory.
     *
     * <p>A list rather than a single directory because the protocol's folder
     * request is answered with a list, and a catalog may reasonably include
     * subdirectories.
     *
     * @param requester the peer who asked
     * @param path the directory's full remote path
     * @return the directories to answer with, empty if there are none
     */
    List<Directory> directory(Username requester, String path);

    /**
     * Returns files matching a peer's search.
     *
     * @param requester the peer who searched
     * @param terms what they searched for
     * @param limit the most files worth returning
     * @return the matches, empty if there are none
     */
    List<SearchFile> search(Username requester, String terms, int limit);

    /**
     * Resolves a file a peer wants to download.
     *
     * @param requester the peer who asked
     * @param path the file's full remote path
     * @return the file, or empty if this requester may not have it
     */
    Optional<ResolvedFile> resolve(Username requester, String path);

    /**
     * Returns what this catalog holds, for the counts announced to the server.
     *
     * @return the index
     */
    ShareIndex index();

    /**
     * A catalog that shares nothing.
     *
     * <p>The default, and a correct client: browsing us returns an empty list
     * rather than an error, our searches match nothing, and every upload request
     * is declined. A leech is a valid participant on this network; a client that
     * throws at every peer who asks is not.
     *
     * @return the empty catalog
     */
    static ShareCatalog empty() {
        return new ShareCatalog() {
            @Override
            public BrowseResponse browse(Username requester) {
                return BrowseResponse.empty();
            }

            @Override
            public List<Directory> directory(Username requester, String path) {
                return List.of();
            }

            @Override
            public List<SearchFile> search(Username requester, String terms, int limit) {
                return List.of();
            }

            @Override
            public Optional<ResolvedFile> resolve(Username requester, String path) {
                return Optional.empty();
            }

            @Override
            public ShareIndex index() {
                return ShareIndex.empty();
            }
        };
    }
}
