// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging;

import dev.slsk.CharacterEncoding;

/**
 * A decoded string and the encoding that succeeded.
 *
 * @param value the decoded value
 * @param encoding the encoding used
 */
public record DecodedString(String value, CharacterEncoding encoding) {}
