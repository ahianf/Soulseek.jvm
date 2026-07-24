// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationSignal;
import dev.slsk.TransferStates;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TransferOptionsTest {
    @Test
    @DisplayName("Instantiates with given data")
    void instantiatesWithGivenData() {
        TransferGovernor governor = (transfer, requested, token) -> CompletableFuture.completedFuture(requested);
        TransferStateChangedCallback stateChanged = change -> {};
        TransferProgressUpdatedCallback progressUpdated = update -> {};
        TransferSlotAwaiter slotAwaiter = (transfer, token) -> CompletableFuture.completedFuture(null);
        TransferSlotReleasedCallback slotReleased = transfer -> {};
        TransferReporter reporter = (transfer, attempted, granted, transferred) -> {};

        TransferOptions options = new TransferOptions(
                governor,
                stateChanged,
                progressUpdated,
                slotAwaiter,
                slotReleased,
                reporter,
                42,
                false,
                false,
                false,
                false);

        assertSame(governor, options.getGovernor());
        assertSame(stateChanged, options.getStateChanged());
        assertSame(progressUpdated, options.getProgressUpdated());
        assertSame(slotAwaiter, options.getSlotAwaiter());
        assertSame(slotReleased, options.getSlotReleased());
        assertSame(reporter, options.getReporter());
        assertEquals(42, options.getMaximumLingerTime());
        assertFalse(options.isSeekInputStreamAutomatically());
        assertFalse(options.isSeekOutputStreamAutomatically());
        assertFalse(options.isDisposeInputStreamOnCompletion());
        assertFalse(options.isDisposeOutputStreamOnCompletion());
    }

    @Test
    @DisplayName("Instantiates with defaults")
    void instantiatesWithDefaults() {
        TransferOptions options = new TransferOptions();

        assertTrue(options.isSeekInputStreamAutomatically());
        assertTrue(options.isSeekOutputStreamAutomatically());
        assertTrue(options.isDisposeInputStreamOnCompletion());
        assertTrue(options.isDisposeOutputStreamOnCompletion());
        assertEquals(TransferOptions.DEFAULT_MAXIMUM_LINGER_TIME, options.getMaximumLingerTime());
        assertEquals(
                Integer.MAX_VALUE,
                options.getGovernor()
                        .grantAsync(null, 1, CancellationSignal.none())
                        .join());
        assertNull(options.getSlotAwaiter()
                .awaitSlotAsync(null, CancellationSignal.none())
                .join());
        assertNull(options.getStateChanged());
        assertNull(options.getProgressUpdated());
        assertNull(options.getSlotReleased());
        assertNull(options.getReporter());
    }

    @Test
    @DisplayName("WithAdditionalStateChanged retains other options")
    void withAdditionalStateChangedRetainsOtherOptions() {
        TransferGovernor governor = (transfer, requested, token) -> CompletableFuture.completedFuture(requested);
        TransferProgressUpdatedCallback progress = update -> {};
        TransferSlotAwaiter awaiter = (transfer, token) -> CompletableFuture.completedFuture(null);
        TransferSlotReleasedCallback released = transfer -> {};
        TransferReporter reporter = (transfer, attempted, granted, transferred) -> {};
        TransferOptions original = new TransferOptions(
                governor, null, progress, awaiter, released, reporter, 42, false, true, false, true);

        TransferOptions copy = original.withAdditionalStateChanged(null);

        assertSame(governor, copy.getGovernor());
        assertSame(progress, copy.getProgressUpdated());
        assertSame(awaiter, copy.getSlotAwaiter());
        assertSame(released, copy.getSlotReleased());
        assertSame(reporter, copy.getReporter());
        assertEquals(42, copy.getMaximumLingerTime());
        assertFalse(copy.isSeekInputStreamAutomatically());
        assertTrue(copy.isSeekOutputStreamAutomatically());
        assertFalse(copy.isDisposeInputStreamOnCompletion());
        assertTrue(copy.isDisposeOutputStreamOnCompletion());
        assertNotSame(original.getStateChanged(), copy.getStateChanged());
        copy.getStateChanged().onStateChanged(new TransferStateChange(TransferStates.NONE, null));
    }

    @Test
    @DisplayName("WithAdditionalStateChanged executes new callback before existing")
    void withAdditionalStateChangedExecutesBothInOrder() {
        List<Integer> order = new ArrayList<>();
        TransferOptions original = new TransferOptions(null, change -> order.add(2));
        TransferOptions copy = original.withAdditionalStateChanged(change -> order.add(1));

        copy.getStateChanged().onStateChanged(new TransferStateChange(TransferStates.NONE, null));

        assertEquals(List.of(1, 2), order);
    }

    @Test
    @DisplayName("WithAdditionalStateChanged stops if new callback throws")
    void withAdditionalStateChangedStopsIfNewCallbackThrows() {
        List<Integer> order = new ArrayList<>();
        RuntimeException failure = new RuntimeException("failure");
        TransferOptions original = new TransferOptions(null, change -> order.add(2));
        TransferOptions copy = original.withAdditionalStateChanged(change -> {
            order.add(1);
            throw failure;
        });

        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> copy.getStateChanged().onStateChanged(new TransferStateChange(TransferStates.NONE, null)));

        assertSame(failure, thrown);
        assertEquals(List.of(1), order);
    }

    @Test
    @DisplayName("WithDisposalOptions retains null overrides")
    void withDisposalOptionsRetainsNullOverrides() {
        TransferOptions original =
                new TransferOptions(null, null, null, null, null, null, 42, false, false, false, true);

        TransferOptions copy = original.withDisposalOptions();

        assertFalse(copy.isDisposeInputStreamOnCompletion());
        assertTrue(copy.isDisposeOutputStreamOnCompletion());
        assertEquals(42, copy.getMaximumLingerTime());
    }

    @Test
    @DisplayName("WithDisposalOptions applies specified overrides")
    void withDisposalOptionsAppliesSpecifiedOverrides() {
        TransferOptions original = new TransferOptions();

        TransferOptions copy = original.withDisposalOptions(false, false);
        TransferOptions outputOnly = original.withDisposalOptions(null, false);

        assertFalse(copy.isDisposeInputStreamOnCompletion());
        assertFalse(copy.isDisposeOutputStreamOnCompletion());
        assertTrue(outputOnly.isDisposeInputStreamOnCompletion());
        assertFalse(outputOnly.isDisposeOutputStreamOnCompletion());
    }

    @Test
    @DisplayName("Tuple adaptations retain values and enforce value-state nullability")
    void tupleAdaptationsRetainValuesAndEnforceStateNullability() {
        TransferStateChange state = new TransferStateChange(TransferStates.COMPLETED, null);
        TransferProgressUpdate progress = new TransferProgressUpdate(42, null);

        assertEquals(TransferStates.COMPLETED, state.previousState());
        assertNull(state.transfer());
        assertEquals(42, progress.previousBytesTransferred());
        assertNull(progress.transfer());
        assertThrows(NullPointerException.class, () -> new TransferStateChange(null, null));
    }
}
