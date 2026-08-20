// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertNull;

import dev.slsk.internal.search.SearchResponseCacheRecord;
import org.junit.jupiter.api.Test;

class SearchResponseCacheRecordTest {
    @Test
    void responseCacheRecordPreservesNullableReferences() {
        SearchResponseCacheRecord record = new SearchResponseCacheRecord(null, -1, null, null);

        assertNull(record.username());
        assertNull(record.query());
        assertNull(record.searchResponse());
    }
}
