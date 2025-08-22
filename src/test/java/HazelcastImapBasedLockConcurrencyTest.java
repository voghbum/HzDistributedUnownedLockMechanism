import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.voghbum.CpSemaphoreUnownedLock;
import com.voghbum.HazelcastImapBasedUnownedLock;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.function.Executable;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import static org.junit.jupiter.api.Assertions.*;

public class HazelcastImapBasedLockConcurrencyTest {
    private static HazelcastInstance hz1, hz2, hz3;
    private static final String LICENSE = System.getenv("HZ_LICENSEKEY");
    private static final String MAP_KEY = "test-map";
    private static final String LOCK_NAME = "test-lock";

    @BeforeAll
    static void setup() {
        Config config = new Config();
        config.setClusterName("test-cluster");
        config.setLicenseKey(LICENSE);
        CPSubsystemConfig cp = new CPSubsystemConfig()
                .setCPMemberCount(3)
                .setGroupSize(3)
                .setSessionHeartbeatIntervalSeconds(1)
                .setSessionTimeToLiveSeconds(15);
        config.setCPSubsystemConfig(cp);

        hz1 = Hazelcast.newHazelcastInstance(config);
        hz2 = Hazelcast.newHazelcastInstance(config);
        hz3 = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll
    static void teardown() {
        hz1.shutdown();
        hz2.shutdown();
        hz3.shutdown();
    }

    @Test
    void shouldUnlockAnotherThreadCorrectlyWhenLowLoad_map() throws InterruptedException {
        IMap<String, String> map = hz1.getMap(MAP_KEY);
        HazelcastImapBasedUnownedLock lock = new HazelcastImapBasedUnownedLock(map, LOCK_NAME);
        shouldUnlockAnotherThreadCorrectly(2, 9, lock);
    }

    @Test
    void shouldUnlockAnotherThreadCorrectlyWhenLowLoad_sem() throws InterruptedException {
        CpSemaphoreUnownedLock lock = new CpSemaphoreUnownedLock(hz1, LOCK_NAME);
        shouldUnlockAnotherThreadCorrectly(2, 9, lock);
    }

    // IT FAILS
    @Test
    void shouldUnlockAnotherThreadCorrectly_map() throws InterruptedException {
        IMap<String, String> map = hz1.getMap(MAP_KEY);
        HazelcastImapBasedUnownedLock lock = new HazelcastImapBasedUnownedLock(map, LOCK_NAME);
        shouldUnlockAnotherThreadCorrectly(100, 500, lock);
    }

    // IT WORKS
    @Test
    void shouldUnlockAnotherThreadCorrectly_sem() throws InterruptedException {
        CpSemaphoreUnownedLock lock = new CpSemaphoreUnownedLock(hz1, LOCK_NAME);
        shouldUnlockAnotherThreadCorrectly(200, 2500, lock);
    }

    private static void shouldUnlockAnotherThreadCorrectly(int threadCount, int taskCount, Lock lock)
            throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        ExecutorService unlockerExecutor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicLong count = new AtomicLong(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (int i = 0; i < taskCount; i++) {
            executor.submit(() -> {
                try {
                    lock.lock();
                    var current = count.incrementAndGet();
                    if (current != 1) {
                        failCount.incrementAndGet();
                    }
                    // Another thread release the lock
                    unlockerExecutor.submit(() -> {
                        try {
                            count.decrementAndGet();
                            lock.unlock();
                        } finally {
                            doneLatch.countDown();
                        }
                    });
                } catch (Exception e) {
                    doneLatch.countDown();
                }
            });
        }

        doneLatch.await();
        executor.shutdown();

        assertEquals(0, failCount.get(), "Critical section violation!");
    }

    @Test
    void shouldReleaseTheLockSameThread() throws InterruptedException {
        IMap<String, String> map = hz1.getMap(MAP_KEY);
        HazelcastImapBasedUnownedLock lock = new HazelcastImapBasedUnownedLock(map, LOCK_NAME);

        int threadCount = 100;
        int taskCount = 500;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicLong count = new AtomicLong(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (int i = 0; i < taskCount; i++) {
            executor.submit(() -> {
                try {
                    lock.lock();
                    var current = count.incrementAndGet();
                    if(current != 1) {
                        failCount.incrementAndGet();
                    }
                    count.decrementAndGet();
                    lock.unlock();
                    doneLatch.countDown();
                } catch (Exception e) {

                    doneLatch.countDown();
                }
            });
        }
        doneLatch.await();
        executor.shutdown();

        assertEquals(0, failCount.get(), "Critical section violation!");
    }
}
