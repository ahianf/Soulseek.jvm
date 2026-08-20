// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.transfer.Transfer;
import dev.slsk.internal.transfer.TransferPhase;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TransferOptionsTest {
    @Test
    @DisplayName("Instantiates with given data")
    void instantiatesWithGivenData() {
        Consumer<TransferStateChange> stateChanged = change -> {};
        Consumer<TransferProgressUpdate> progressUpdated = update -> {};
        Consumer<Transfer> slotReleased = transfer -> {};
        TransferReporter reporter = (transfer, attempted, granted, transferred) -> {};

        TransferOptions options = TransferOptions.builder()
                .stateChanged(stateChanged)
                .progressUpdated(progressUpdated)
                .slotReleased(slotReleased)
                .reporter(reporter)
                .maximumLingerTime(Duration.ofMillis(42))
                .seekInputStreamAutomatically(false)
                .seekOutputStreamAutomatically(false)
                .disposeInputStreamOnCompletion(false)
                .disposeOutputStreamOnCompletion(false)
                .build();

        assertSame(stateChanged, options.stateChanged());
        assertSame(progressUpdated, options.progressUpdated());
        assertSame(slotReleased, options.slotReleased());
        assertSame(reporter, options.reporter());
        assertEquals(Duration.ofMillis(42), options.maximumLingerTime());
        assertFalse(options.seekInputStreamAutomatically());
        assertFalse(options.seekOutputStreamAutomatically());
        assertFalse(options.disposeInputStreamOnCompletion());
        assertFalse(options.disposeOutputStreamOnCompletion());
    }

    @Test
    @DisplayName("Instantiates with defaults")
    void instantiatesWithDefaults() {
        TransferOptions options = new TransferOptions();

        assertTrue(options.seekInputStreamAutomatically());
        assertTrue(options.seekOutputStreamAutomatically());
        assertTrue(options.disposeInputStreamOnCompletion());
        assertTrue(options.disposeOutputStreamOnCompletion());
        assertEquals(TransferOptions.DEFAULT_MAXIMUM_LINGER_TIME, options.maximumLingerTime());
        assertNull(options.stateChanged());
        assertNull(options.progressUpdated());
        assertNull(options.slotReleased());
        assertNull(options.reporter());
    }

    @Test
    @DisplayName("WithAdditionalStateChanged retains other options")
    void withAdditionalStateChangedRetainsOtherOptions() {
        Consumer<TransferProgressUpdate> progress = update -> {};
        Consumer<Transfer> released = transfer -> {};
        TransferReporter reporter = (transfer, attempted, granted, transferred) -> {};
        TransferOptions original = TransferOptions.builder()
                .progressUpdated(progress)
                .slotReleased(released)
                .reporter(reporter)
                .maximumLingerTime(Duration.ofMillis(42))
                .seekInputStreamAutomatically(false)
                .disposeInputStreamOnCompletion(false)
                .build();

        TransferOptions copy = original.withAdditionalStateChanged(null);

        assertSame(progress, copy.progressUpdated());
        assertSame(released, copy.slotReleased());
        assertSame(reporter, copy.reporter());
        assertEquals(Duration.ofMillis(42), copy.maximumLingerTime());
        assertFalse(copy.seekInputStreamAutomatically());
        assertTrue(copy.seekOutputStreamAutomatically());
        assertFalse(copy.disposeInputStreamOnCompletion());
        assertTrue(copy.disposeOutputStreamOnCompletion());
        assertNotSame(original.stateChanged(), copy.stateChanged());
        copy.stateChanged().accept(new TransferStateChange(TransferPhase.NONE, null));
    }

    @Test
    @DisplayName("WithAdditionalStateChanged executes new callback before existing")
    void withAdditionalStateChangedExecutesBothInOrder() {
        List<Integer> order = new ArrayList<>();
        TransferOptions original =
                TransferOptions.builder().stateChanged(change -> order.add(2)).build();
        TransferOptions copy = original.withAdditionalStateChanged(change -> order.add(1));

        copy.stateChanged().accept(new TransferStateChange(TransferPhase.NONE, null));

        assertEquals(List.of(1, 2), order);
    }

    @Test
    @DisplayName("WithAdditionalStateChanged stops if new callback throws")
    void withAdditionalStateChangedStopsIfNewCallbackThrows() {
        List<Integer> order = new ArrayList<>();
        RuntimeException failure = new RuntimeException("failure");
        TransferOptions original = TransferOptions.builder()
                .progressUpdated(change -> order.add(2))
                .build();
        TransferOptions copy = original.withAdditionalStateChanged(change -> {
            order.add(1);
            throw failure;
        });

        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> copy.stateChanged().accept(new TransferStateChange(TransferPhase.NONE, null)));

        assertSame(failure, thrown);
        assertEquals(List.of(1), order);
    }

    @Test
    @DisplayName("WithDisposalOptions retains null overrides")
    void withDisposalOptionsRetainsNullOverrides() {
        TransferOptions original = TransferOptions.builder()
                .maximumLingerTime(Duration.ofMillis(42))
                .seekInputStreamAutomatically(false)
                .seekOutputStreamAutomatically(false)
                .disposeInputStreamOnCompletion(false)
                .build();

        TransferOptions copy = original.withDisposalOptions();

        assertFalse(copy.disposeInputStreamOnCompletion());
        assertTrue(copy.disposeOutputStreamOnCompletion());
        assertEquals(Duration.ofMillis(42), copy.maximumLingerTime());
    }

    @Test
    @DisplayName("WithDisposalOptions applies specified overrides")
    void withDisposalOptionsAppliesSpecifiedOverrides() {
        TransferOptions original = new TransferOptions();

        TransferOptions copy = original.withDisposalOptions(false, false);
        TransferOptions outputOnly = original.withDisposalOptions(null, false);

        assertFalse(copy.disposeInputStreamOnCompletion());
        assertFalse(copy.disposeOutputStreamOnCompletion());
        assertTrue(outputOnly.disposeInputStreamOnCompletion());
        assertFalse(outputOnly.disposeOutputStreamOnCompletion());
    }

    @Test
    @DisplayName("Tuple adaptations retain values and enforce value-state nullability")
    void tupleAdaptationsRetainValuesAndEnforceStateNullability() {
        TransferStateChange state = new TransferStateChange(TransferPhase.COMPLETED, null);
        TransferProgressUpdate progress = new TransferProgressUpdate(42, null);

        assertEquals(TransferPhase.COMPLETED, state.previousState());
        assertNull(state.transfer());
        assertEquals(42, progress.previousBytesTransferred());
        assertNull(progress.transfer());
        assertThrows(NullPointerException.class, () -> new TransferStateChange(null, null));
    }
}
