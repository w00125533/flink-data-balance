package com.fdb.simulator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RateController {

    private final long tokensPerSecond;
    private final ConcurrentHashMap<String, TokenBucket> perCellBuckets = new ConcurrentHashMap<>();

    public RateController(long tokensPerSecond) {
        this.tokensPerSecond = tokensPerSecond;
    }

    public boolean tryAcquire(String cellId) {
        return perCellBuckets
            .computeIfAbsent(cellId, k -> new TokenBucket(tokensPerSecond))
            .tryAcquire();
    }

    static class TokenBucket {
        private final long capacity;
        private final double refillPerNano;
        private final AtomicLong tokens;
        private volatile long lastRefillNanos;

        TokenBucket(long ratePerSecond) {
            this.capacity = ratePerSecond;
            this.refillPerNano = ratePerSecond / 1_000_000_000.0;
            this.tokens = new AtomicLong(ratePerSecond);
            this.lastRefillNanos = System.nanoTime();
        }

        boolean tryAcquire() {
            refill();
            while (true) {
                long current = tokens.get();
                if (current <= 0) return false;
                if (tokens.compareAndSet(current, current - 1)) return true;
            }
        }

        private void refill() {
            long now = System.nanoTime();
            long elapsed = now - lastRefillNanos;
            if (elapsed > 0) {
                lastRefillNanos = now;
                long added = (long) (elapsed * refillPerNano);
                if (added > 0) {
                    tokens.updateAndGet(t -> Math.min(t + added, capacity));
                }
            }
        }
    }
}
