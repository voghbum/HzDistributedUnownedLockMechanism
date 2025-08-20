package com.voghbum;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

public final class CpBasedUnownedLock implements Lock {
    private final IAtomicLong state; // 0 = free, 1 = locked
    private final String name;

    public CpBasedUnownedLock(HazelcastInstance hz, String name) {
        this.name = name;
        this.state = hz.getCPSubsystem().getAtomicLong(name);
        this.state.compareAndSet(0, 0);
    }

    @Override
    public void lock() {
        long nanos = TimeUnit.MICROSECONDS.toNanos(50);
        final long cap = TimeUnit.MILLISECONDS.toNanos(5);
        for (;;) {
            if (tryLock()) return;
            LockSupport.parkNanos(nanos);
            nanos = Math.min(cap, (long) (nanos * 1.5)) + (long) (Math.random() * 100_000);
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock() {
        return state.compareAndSet(0, 1);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        final long deadline = System.nanoTime() + unit.toNanos(time);
        long nanos = TimeUnit.MICROSECONDS.toNanos(50);
        final long cap = TimeUnit.MILLISECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (tryLock()) return true;
            LockSupport.parkNanos(nanos);
            if (Thread.interrupted()) throw new InterruptedException();
            nanos = Math.min(cap, (long) (nanos * 1.5)) + (long) (Math.random() * 100_000);
        }
        return false;
    }

    @Override
    public void unlock() {
        state.compareAndSet(1, 0);
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException("Conditions are not supported in CpUnownedLock");
    }

    public boolean isLocked() {
        return state.get() == 1;
    }

    @Override
    public String toString() {
        return "CpUnownedLock[" + name + "]";
    }
}
