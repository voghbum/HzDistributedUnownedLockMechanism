import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.voghbum.HazelcastImapBasedUnownedLock;
import org.junit.jupiter.api.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class HazelcastImapBasedLockConcurrencyTest {
    private static HazelcastInstance hazelcastInstance;
    private static final String LICENSE = "ENTER LICENSE KEY";
    private static final String MAP_KEY = "test-map";
    private static final String LOCK_NAME = "test-lock";

    @BeforeAll
    static void setup() {
        Config config = new Config();
        config.setClusterName("test-cluster");
        config.setLicenseKey(LICENSE);
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll
    static void teardown() {
        hazelcastInstance.shutdown();
    }

    @Test
    void shouldUnlockAnotherThreadCorrectlyWhenLowLoad() throws InterruptedException {
        IMap<String, String> map = hazelcastInstance.getMap(MAP_KEY);
        HazelcastImapBasedUnownedLock lock = new HazelcastImapBasedUnownedLock(map, LOCK_NAME);
        int threadCount = 2;
        int taskCount = 9;
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
                    if(current != 1) {
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

    // IT FAILS
    @Test
    void shouldUnlockAnotherThreadCorrectly() throws InterruptedException {
        IMap<String, String> map = hazelcastInstance.getMap(MAP_KEY);
        HazelcastImapBasedUnownedLock lock = new HazelcastImapBasedUnownedLock(map, LOCK_NAME);
        int threadCount = 100;
        int taskCount = 500;
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
                    if(current != 1) {
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
        IMap<String, String> map = hazelcastInstance.getMap(MAP_KEY);
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
