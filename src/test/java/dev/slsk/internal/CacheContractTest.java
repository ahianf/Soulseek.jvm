// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class CacheContractTest {
    @Test
    void lookupResultDistinguishesAbsentFromPresentNull() {
        CacheLookupResult<String> absent = CacheLookupResult.notFound();
        CacheLookupResult<String> presentNull = CacheLookupResult.found(null);

        assertFalse(absent.found());
        assertNull(absent.value());
        assertTrue(presentNull.found());
        assertNull(presentNull.value());
    }

    @Test
    void responseCacheRecordPreservesNullableReferences() {
        SearchResponseCacheRecord record = new SearchResponseCacheRecord(null, -1, null, null);

        assertNull(record.username());
        assertNull(record.query());
        assertNull(record.searchResponse());
    }
}
