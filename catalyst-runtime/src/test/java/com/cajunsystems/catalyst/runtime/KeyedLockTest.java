package com.cajunsystems.catalyst.runtime;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeyedLockTest {

    @Test
    void sameKeyIsMutuallyExclusive() throws Exception {
        KeyedLock<String> locks = new KeyedLock<>();
        int threads = 16, perThread = 500;
        // Deliberately NOT atomic: only mutual exclusion can make this count come out right.
        int[] counter = {0};
        CountDownLatch start = new CountDownLatch(1);

        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            CountDownLatch done = new CountDownLatch(threads);
            for (int i = 0; i < threads; i++) {
                pool.execute(() -> {
                    try {
                        start.await();
                        for (int j = 0; j < perThread; j++) {
                            locks.withLock("same", () -> counter[0]++);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(counter[0]).isEqualTo(threads * perThread);
    }

    @Test
    void differentKeysDoNotBlockEachOther() throws Exception {
        // The whole point of the type: two holders of different keys must be able to be inside their
        // sections at the same time. A barrier proves it — under a single global lock the first holder
        // would block forever waiting for a second that can never enter.
        KeyedLock<String> locks = new KeyedLock<>();
        CyclicBarrier bothInside = new CyclicBarrier(2);

        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            CountDownLatch done = new CountDownLatch(2);
            AtomicInteger passed = new AtomicInteger();
            for (String key : new String[]{"a", "b"}) {
                pool.execute(() -> {
                    locks.withLock(key, () -> {
                        try {
                            bothInside.await(10, TimeUnit.SECONDS);
                            passed.incrementAndGet();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                    done.countDown();
                });
            }
            assertThat(done.await(15, TimeUnit.SECONDS)).isTrue();
            assertThat(passed).hasValue(2);
        }
    }

    @Test
    void isReentrantForTheSameKey() {
        KeyedLock<String> locks = new KeyedLock<>();
        // A section that re-enters its own key must not self-deadlock.
        String result = locks.withLock("k", () -> locks.withLock("k", () -> "inner"));
        assertThat(result).isEqualTo("inner");
        assertThat(locks.trackedKeys()).isZero();
    }

    @Test
    void releasesTheLockWhenTheBodyThrows() {
        KeyedLock<String> locks = new KeyedLock<>();
        assertThatThrownBy(() -> locks.withLock("k", () -> {
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);

        // The entry is gone and the key is usable again — a throwing section must not wedge it.
        assertThat(locks.trackedKeys()).isZero();
        assertThat(locks.withLock("k", () -> "ok")).isEqualTo("ok");
    }

    @Test
    void doesNotRetainLocksForIdleKeys() throws Exception {
        // Locks are created per key on demand; a runtime that has scheduled many executions must not
        // accumulate one lock per id forever.
        KeyedLock<Integer> locks = new KeyedLock<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(8)) {
            CountDownLatch done = new CountDownLatch(1000);
            for (int i = 0; i < 1000; i++) {
                int key = i;
                pool.execute(() -> {
                    locks.withLock(key, () -> key * 2);
                    done.countDown();
                });
            }
            assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(locks.trackedKeys()).isZero();
    }
}
