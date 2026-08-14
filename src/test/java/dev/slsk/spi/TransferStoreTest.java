// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.download.Download;
import dev.slsk.transfer.Priority;
import dev.slsk.transfer.Progress;
import dev.slsk.transfer.TransferId;
import dev.slsk.transfer.TransferState;
import dev.slsk.user.Username;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The default store, which is the contract every other implementation copies. */
class TransferStoreTest {

    private static Download download(String id, TransferState state) {
        return new Download(
                TransferId.of(id),
                Username.of("alice"),
                "music\\song.mp3",
                1024,
                state,
                Priority.NORMAL,
                Instant.EPOCH,
                Optional.empty(),
                Optional.empty(),
                1,
                Map.of());
    }

    @Test
    void savesAndLoadsWhatWasQueued() {
        TransferStore store = TransferStore.inMemory();
        store.save(download("one", new TransferState.Queued(0)));
        store.save(download("two", new TransferState.Requesting()));

        assertEquals(
                List.of(TransferId.of("one"), TransferId.of("two")),
                store.loadAll().stream().map(Download::id).toList());
    }

    @Test
    @DisplayName("saving the same id again replaces the record rather than duplicating it")
    void saveIsKeyedOnTheId() {
        TransferStore store = TransferStore.inMemory();
        store.save(download("one", new TransferState.Queued(0)));
        TransferState.Transferring transferring =
                new TransferState.Transferring(new Progress(512, 1024, 128.0, Optional.empty()));
        store.save(download("one", transferring));

        assertEquals(1, store.loadAll().size());
        assertEquals(transferring, store.loadAll().getFirst().state());
    }

    @Test
    void deleteForgetsOneAndLeavesTheRest() {
        TransferStore store = TransferStore.inMemory();
        store.save(download("one", new TransferState.Queued(0)));
        store.save(download("two", new TransferState.Queued(1)));

        store.delete(TransferId.of("one"));

        assertEquals(
                List.of(TransferId.of("two")),
                store.loadAll().stream().map(Download::id).toList());
    }

    @Test
    void deletingSomethingUnknownIsANoOp() {
        TransferStore store = TransferStore.inMemory();
        store.delete(TransferId.of("never-there"));
        assertEquals(List.of(), store.loadAll());
    }

    @Test
    @DisplayName("loadAll hands back a snapshot, not a view that changes underneath")
    void loadAllIsASnapshot() {
        TransferStore store = TransferStore.inMemory();
        store.save(download("one", new TransferState.Queued(0)));

        List<Download> loaded = store.loadAll();
        store.save(download("two", new TransferState.Queued(1)));

        assertEquals(1, loaded.size());
        assertEquals(2, store.loadAll().size());
    }

    @Test
    void rejectsNulls() {
        TransferStore store = TransferStore.inMemory();
        assertThrows(NullPointerException.class, () -> store.save(null));
        assertThrows(NullPointerException.class, () -> store.delete(null));
    }
}
