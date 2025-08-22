package com.voghbum;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.ISemaphore;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * The problem with AtomicLong impl is that AtomicLongs, in Hazelcast, is not bound to the CP Session nor they have a lease time.
 * This may create problems because:
 * - no client session: if the lock holder crashes the long value of 1 stays there forever (it's a lock leak)
 * - no lease: if you forget to unlock, it never releases the lock
 * And, there's no fencing, so it may be that if client1 holds the lock and then it's delayed, over the cap,
 * then the lock is released by the system. Now client2 acquires the lock. If client1 wakes up it still thinks
 * he has the lock and can access resources whilst client2 has the lock creating a data corruption problem.
 *
 * The issue with the IMap implementation simulating unowned lock is that IMap locks are data-partition locks (AP),
 * thread/caller-owned, and not CP/linearizable.
 * Using forceUnlock() bypasses ownership checks and introduces races under load as your test shows
 * You'll also have problems because there's no lease so this may create leaks
 *
 * With ISemaphore from the CP system (see https://docs.hazelcast.org/docs/latest/javadoc/com/hazelcast/cp/ISemaphore.html) set with 1 permit only:
 * - the acquire/release is agreed by all members of the CP subsystem
 * - it is session-aware so
 *   - clients within the same session can lock/unlock; this includes two distinct threads within the same client application
 *   - if a client holding a lock dies, the session is cleared and locks automatically released
 * - lineariazbility is guaranteed via RAFT so under load mutual exclusion is guaranteed
 * this works also between two hazelcast clients connected to the same
 *
 * two threads in the same client application belong to the same session so it's the app's responsibility to correcly program locks/unlocks
 *
 * two clients connected to the same CP subsystem (ie distinct hazelcast clients) form two distinct CP sessions:
 * - clientA acquires (CP knows that A holds 1 permit)
 * - A dies and CP reclaims lock
 * - clientB acquires lock (only B session can release)
 * - if A later calls release() CP rejects it
 *
 */
public final class CpSemaphoreUnownedLock implements Lock {
    private final ISemaphore sem;
    private final String name;

    public CpSemaphoreUnownedLock(HazelcastInstance hz, String name) {
        this.name = Objects.requireNonNull(name, "name");
        Objects.requireNonNull(hz, "hz");
        this.sem = hz.getCPSubsystem().getSemaphore(name);
        // initialize to a binary semaphore (only once cluster-wide)
        sem.init(1); // returns false if already initialized; that's fine
    }

    @Override
    public void lock() {
        boolean interrupted = false;
        for (;;) {
            try {
                sem.acquire();                // blocks until a permit is available
                break;
            } catch (InterruptedException ie) {
                // Lock.lock() must be uninterruptible: remember the interrupt and keep waiting
                interrupted = true;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        sem.acquire();                        // interruptible variant
    }

    @Override
    public boolean tryLock() {
        return sem.tryAcquire();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        if (time <= 0) return sem.tryAcquire();
        return sem.tryAcquire(time, unit);
    }

    @Override
    public void unlock() {
        try {
            sem.release();                    // may be called by a different thread of the SAME HazelcastInstance
        } catch (IllegalStateException e) {
            // no permit held by this Hazelcast "caller" (session-aware mode)
            throw new IllegalMonitorStateException("No permit held by this Hazelcast instance");
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException("Conditions are not supported");
    }

    @Override
    public String toString() {
        return "CpSemaphoreUnownedLock[" + name + "]";
    }
}
