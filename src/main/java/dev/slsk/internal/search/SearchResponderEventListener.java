// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import dev.slsk.internal.events.SoulseekClientEvent;
import java.util.function.Consumer;

/** Handles a search-responder event. */
@FunctionalInterface
public interface SearchResponderEventListener<T extends SoulseekClientEvent> extends Consumer<T> {}
