// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import java.util.function.Consumer;

/** Handles a distributed connection-manager event. */
@FunctionalInterface
public interface DistributedManagerEventListener<T> extends Consumer<T> {}
