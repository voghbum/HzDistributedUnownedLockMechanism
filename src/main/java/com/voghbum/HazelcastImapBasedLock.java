package com.voghbum;

import com.hazelcast.map.IMap;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Imap based implementation of Distributed Lock. This implementation is needed due to memory usage
 * problems of ILock object. When the ILock instance is unlocked, it continues to keep the relevant
 * memory in the heap. The only way to remove memory from the heap is to destroy corresponding
 * instance. Ann "Already Destroyed" exception is encountered when trying to obtain a key with the
 * same name after the related instance has been destroyed.
 */
public class HazelcastImapBasedLock implements Lock {

    private static final int LEASE_TIME_MULTIPLY_RATE = 5;

    /**
     * Lock operation is perform through this map.
     */
    private final IMap<String, String> lockMap;

    /**
     * Name of lock.
     */
    private final String lockName;

    /**
     * Construct a new {@link HazelcastImapBasedLock}.
     *
     * @param lockMap  Lock operation is perform through this map.
     * @param lockName Name of lock.
     */
    public HazelcastImapBasedLock(IMap<String, String> lockMap, String lockName) {
        Objects.requireNonNull(lockMap, "Lock map can not be null!");
        Objects.requireNonNull(lockName, "Lock name can not be null!");
        this.lockMap = lockMap;
        this.lockName = lockName;
    }

    @Override
    public void lock() {
        lockMap.lock(lockName);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException("Interruptibly locking is not supported.");
    }

    @Override
    public boolean tryLock() {
        return lockMap.tryLock(lockName);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return lockMap.tryLock(lockName, time, unit, time * LEASE_TIME_MULTIPLY_RATE, unit);
    }

    @Override
    public void unlock() {
            lockMap.forceUnlock(lockName);
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException(
                "Imap based lock implementation does not support conditions.");
    }
}