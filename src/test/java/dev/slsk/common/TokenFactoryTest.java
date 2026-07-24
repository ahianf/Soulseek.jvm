// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TokenFactoryTest {
    @Test
    @DisplayName("First token is the configured start")
    void firstTokenIsStart() {
        assertEquals(42, new TokenFactory(42).nextToken());
        assertEquals(0, new TokenFactory().nextToken());
    }

    @Test
    @DisplayName("Returns sequential tokens")
    void returnsSequentialTokens() {
        TokenFactory factory = new TokenFactory(-2);

        assertEquals(-2, factory.nextToken());
        assertEquals(-1, factory.nextToken());
        assertEquals(0, factory.nextToken());
    }

    @Test
    @DisplayName("Rolls over at Integer.MAX_VALUE")
    void rollsOverAtMaximumValue() {
        TokenFactory factory = new TokenFactory(Integer.MAX_VALUE);

        assertEquals(Integer.MAX_VALUE, factory.nextToken());
        assertEquals(0, factory.nextToken());
        assertEquals(1, factory.nextToken());
    }

    @Test
    @DisplayName("Concurrent calls return one contiguous sequence")
    void concurrentCallsReturnContiguousSequence() throws InterruptedException {
        int threadCount = 8;
        int tokensPerThread = 1_000;
        TokenFactory factory = new TokenFactory();
        List<Integer> tokens = Collections.synchronizedList(new ArrayList<>());
        List<Thread> threads = new ArrayList<>();

        for (int thread = 0; thread < threadCount; thread++) {
            threads.add(Thread.ofPlatform().start(() -> {
                for (int token = 0; token < tokensPerThread; token++) {
                    tokens.add(factory.nextToken());
                }
            }));
        }
        for (Thread thread : threads) {
            thread.join();
        }

        Collections.sort(tokens);
        assertEquals(threadCount * tokensPerThread, tokens.size());
        for (int token = 0; token < tokens.size(); token++) {
            assertEquals(token, tokens.get(token));
        }
    }
}
