// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import java.util.function.Consumer;

/** Handles an accepted TCP connection. */
@FunctionalInterface
public interface ListenerAcceptedEventListener extends Consumer<Connection> {}
