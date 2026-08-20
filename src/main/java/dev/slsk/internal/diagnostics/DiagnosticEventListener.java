// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.diagnostics;

import java.util.function.Consumer;

/**
 * Handles an internally generated diagnostic message.
 *
 * <p>The payload identifies its source, so listeners do not need a separate
 * sender argument.
 */
@FunctionalInterface
public interface DiagnosticEventListener extends Consumer<DiagnosticEvent> {}
