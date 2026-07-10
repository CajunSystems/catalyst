package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** A {@link ExecutionHandle} backed by a {@link CompletableFuture}. */
final class FutureExecutionHandle<R> implements ExecutionHandle<R> {

    private final ExecutionId id;
    private final CompletableFuture<R> future;

    FutureExecutionHandle(ExecutionId id, CompletableFuture<R> future) {
        this.id = id;
        this.future = future;
    }

    @Override
    public ExecutionId id() {
        return id;
    }

    @Override
    public R result() {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted awaiting execution " + id, e);
        } catch (ExecutionException e) {
            throw unwrap(e.getCause());
        }
    }

    @Override
    public R result(Duration timeout) {
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted awaiting execution " + id, e);
        } catch (ExecutionException e) {
            throw unwrap(e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException("Timed out after " + timeout + " awaiting execution " + id, e);
        }
    }

    private RuntimeException unwrap(Throwable cause) {
        if (cause instanceof RuntimeException re) return re;
        if (cause instanceof Error err) throw err;
        return new CompletionException(cause);
    }
}
