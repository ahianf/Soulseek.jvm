// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

/**
 * An undelivered search response and its routing context.
 *
 * @param username the destination username
 * @param token the original search token
 * @param query the original search query text
 * @param searchResponse the response
 */
public record SearchResponseCacheRecord(String username, int token, String query, SearchResponse searchResponse) {}
