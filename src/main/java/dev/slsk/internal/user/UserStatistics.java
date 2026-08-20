// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.user;

/** User statistics. */
public record UserStatistics(String username, int averageSpeed, long uploadCount, int fileCount, int directoryCount) {}
