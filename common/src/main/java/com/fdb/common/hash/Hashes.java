package com.fdb.common.hash;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

public final class Hashes {

    private static final HashFunction MURMUR3_128 = Hashing.murmur3_128(0xC0FFEE);

    private Hashes() {}

    public static long murmur3(String key) {
        return MURMUR3_128.hashString(key, StandardCharsets.UTF_8).asLong();
    }

    public static int toVBucket(String key, int numVBuckets) {
        if (numVBuckets <= 0) {
            throw new IllegalArgumentException("numVBuckets must be positive, got " + numVBuckets);
        }
        long h = murmur3(key);
        return (int) Math.floorMod(h, numVBuckets);
    }

    public static int toVBucketWithShift(String key, int numVBuckets, int slotShift) {
        if (numVBuckets <= 0) {
            throw new IllegalArgumentException("numVBuckets must be positive, got " + numVBuckets);
        }
        if (Integer.bitCount(numVBuckets) != 1) {
            throw new IllegalArgumentException("numVBuckets must be a power of 2 for XOR shift, got " + numVBuckets);
        }
        int base = toVBucket(key, numVBuckets);
        return (base ^ slotShift) & (numVBuckets - 1);
    }
}
