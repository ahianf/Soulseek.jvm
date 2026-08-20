// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import java.util.function.Consumer;

/** Handles a server-message-handler event. */
@FunctionalInterface
public interface ServerMessageHandlerEventListener<T> extends Consumer<T> {}
