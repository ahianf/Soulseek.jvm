// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.diagnostics;

import dev.slsk.Subscription;
import java.util.function.Consumer;

/** Generates diagnostic message events. */
public interface DiagnosticSource {
    /** Subscribes to generated diagnostics. */
    Subscription subscribe(Consumer<? super DiagnosticMessage> listener);
}
