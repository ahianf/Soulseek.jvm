// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import java.util.function.Consumer;

/** Handles a message-connection event. */
@FunctionalInterface
public interface MessageConnectionEventListener<T extends MessageConnectionEvent> extends Consumer<T> {}
