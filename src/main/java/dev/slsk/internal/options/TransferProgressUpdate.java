// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import dev.slsk.internal.Transfer;

/**
 * A transfer progress callback payload.
 *
 * @param previousBytesTransferred the previous transferred byte count
 * @param transfer the transfer after the progress update
 */
public record TransferProgressUpdate(long previousBytesTransferred, Transfer transfer) {}
