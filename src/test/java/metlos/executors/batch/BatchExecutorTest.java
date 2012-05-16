/*
 * RHQ Management Platform
 * Copyright (C) 2005-2012 Red Hat, Inc.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package metlos.executors.batch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * 
 *
 * @author Lukas Krejci
 */
@Test
public class BatchExecutorTest {

    private static final Log LOG  = LogFactory.getLog(BatchExecutorTest.class);
    
    public void testTimingOfFewTasks_SingleThreaded() throws Exception {
        int nofThreads = 1;
        int nofJobs = 10;
        int taskDurationMillis = 100;
        
        long minimalDuration = rapidFireSimpleExecutorTime(taskDurationMillis, nofJobs, nofThreads);
        
        BatchExecutor ex = getExecutor(nofThreads);
        
        List<Callable<Void>> tasks = getCallables(taskDurationMillis, nofJobs);
        long expectedDuration = minimalDuration * 2;
        long actualDuration = measureExecutionTime(System.currentTimeMillis(), ex.invokeAllWithin(tasks, expectedDuration, TimeUnit.MILLISECONDS));
        
        long min = (long) (expectedDuration * 0.85);
        long max = (long) (expectedDuration * 1.15);
        
        LOG.info("testTimingOfFewTasks_SingleThreaded() stats: expectedDuration=" + expectedDuration + ", actualDuration=" + actualDuration + ", diff=" + (actualDuration - expectedDuration)); 
        assert actualDuration > min && actualDuration < max : "Duration should have been something between " + min + " and " + max + "ms (ideally " + expectedDuration + ") but was " + actualDuration + "ms.";
    }

    public void testTimingApproachingLimitNumberOfTasks_SingleThreaded() throws Exception {
        int nofThreads = 1;
        int nofJobs = 10;
        int taskDurationMillis = 100;
        
        long minimalDuration = rapidFireSimpleExecutorTime(taskDurationMillis, nofJobs, nofThreads);
        
        BatchExecutor ex = getExecutor(nofThreads);
        
        List<Callable<Void>> tasks = getCallables(taskDurationMillis, nofJobs);
        long expectedDuration = minimalDuration;
        long actualDuration = measureExecutionTime(System.currentTimeMillis(), ex.invokeAllWithin(tasks, expectedDuration, TimeUnit.MILLISECONDS));
        
        long min = (long) (expectedDuration * 0.85);
        long max = (long) (expectedDuration * 1.15);
        
        LOG.info("testTimingApproachingLimitNumberOfTasks_SingleThreaded() stats: expectedDuration=" + expectedDuration + ", actualDuration=" + actualDuration + ", diff=" + (actualDuration - expectedDuration)); 
        assert actualDuration > min && actualDuration < max : "Duration should have been something between " + min + " and " + max + "ms (ideally " + expectedDuration + ") but was " + actualDuration + "ms.";
    }
    
    public void testTimingOfFewTasks_MultiThreaded() throws Exception {
        int nofThreads = 15;
        int nofJobs = 100;
        int taskDurationMillis = 100;
        
        long minimalDuration = rapidFireSimpleExecutorTime(taskDurationMillis, nofJobs, nofThreads);
        
        BatchExecutor ex = getExecutor(nofThreads);
        
        List<Callable<Void>> tasks = getCallables(taskDurationMillis, nofJobs);
        long expectedDuration = minimalDuration * 2;
        long actualDuration = measureExecutionTime(System.currentTimeMillis(), ex.invokeAllWithin(tasks, expectedDuration, TimeUnit.MILLISECONDS));
        
        long min = (long) (expectedDuration * 0.85);
        long max = (long) (expectedDuration * 1.15);
        
        LOG.info("testTimingOfFewTasks_MultiThreaded() stats: expectedDuration=" + expectedDuration + ", actualDuration=" + actualDuration + ", diff=" + (actualDuration - expectedDuration)); 
        assert actualDuration > min && actualDuration < max : "Duration should have been something between " + min + " and " + max + "ms (ideally " + expectedDuration + ") but was " + actualDuration + "ms.";
    }

    public void testTimingApproachingLimitNumberOfTasks_MultiThreaded() throws Exception {
        int nofThreads = 15;
        int nofJobs = 100;
        int taskDurationMillis = 100;
        
        long minimalDuration = rapidFireSimpleExecutorTime(taskDurationMillis, nofJobs, nofThreads);
        
        BatchExecutor ex = getExecutor(nofThreads);
        
        List<Callable<Void>> tasks = getCallables(taskDurationMillis, nofJobs);
        long expectedDuration = minimalDuration;
        long actualDuration = measureExecutionTime(System.currentTimeMillis(), ex.invokeAllWithin(tasks, expectedDuration, TimeUnit.MILLISECONDS));
        
        long min = (long) (expectedDuration * 0.85);
        long max = (long) (expectedDuration * 1.15);
        
        LOG.info("testTimingApproachingLimitNumberOfTasks_MultiThreaded() stats: expectedDuration=" + expectedDuration + ", actualDuration=" + actualDuration + ", diff=" + (actualDuration - expectedDuration)); 
        assert actualDuration > min && actualDuration < max : "Duration should have been something between " + min + " and " + max + "ms (ideally " + expectedDuration + ") but was " + actualDuration + "ms.";
    }

    public void testRepetitionOfTasks_SingleThreaded() throws Exception {
        runSimpleDelayTest(1);
    }
    
    public void testRepetitionOfTasks_MultiThreaded() throws Exception {
        runSimpleDelayTest(10);
    }
    
    public void testChangesInTaskCollectionPickedUpInRepetitions() throws Exception {
        final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<Runnable>();
        final AtomicInteger reportedNofTasks = new AtomicInteger();
        final CountDownLatch waitForTask2 = new CountDownLatch(2);
        final CountDownLatch waitForTask3 = new CountDownLatch(2);
        
        Runnable task1 = new Runnable() {
            @Override
            public void run() {
            }
        };
        
        Runnable task2 = new Runnable() {
            @Override
            public void run() {
                if (tasks.size() == 2) {
                    reportedNofTasks.set(2);
                    waitForTask2.countDown();
                }
            }
        };
        
        Runnable task3 = new Runnable() {
            @Override
            public void run() {
                if (tasks.size() == 3) {
                    reportedNofTasks.set(3);
                    waitForTask3.countDown();
                }
            }
        };
        
        BatchExecutor ex = getExecutor(10);
        
        tasks.add(task1);
        tasks.add(task2);
        
        ex.submitWithPreferedDurationAndFixedDelay(tasks, 0, 0, 0, TimeUnit.MILLISECONDS);
        
        //k, now the tasks should be running and there should be just 2 of them...
        //so we should be getting the value of "2" reported by the reportedNofTasks
        
        waitForTask2.countDown();
        waitForTask2.await();
        
        int currentReportedTasks = reportedNofTasks.get();
        
        assert currentReportedTasks == 2 : "We should be getting 2 tasks reported but are getting " + currentReportedTasks + " instead.";        

        //k, now let's try updating the tasks collection... this should get picked up by the
        //repeated executions 
        tasks.add(task3);
        
        //now the reported nof tasks should change to 3. let's wait on it first to make sure the executor has had time to 
        //register the change.        
        waitForTask3.countDown();
        waitForTask3.await();
        
        currentReportedTasks = reportedNofTasks.get();
        
        assert currentReportedTasks == 3 : "We should be getting 3 tasks reported but are getting " + currentReportedTasks + " instead.";
        
        ex.shutdown();
    }
    
    private void runSimpleDelayTest(int nofThreads) throws Exception {
        final ConcurrentLinkedQueue<Long> executionTimes = new ConcurrentLinkedQueue<Long>();

        Runnable task = new Runnable() {
            @Override
            public void run() {
                executionTimes.add(System.currentTimeMillis());
            }
        };

        BatchExecutor ex = getExecutor(nofThreads);

        //start running my task... the task should "take" 0ms and there should be a delay
        //of 10ms between executions... the executionTimes collection should therefore
        //contain time stamps 10ms apart from each other.
        ex.submitWithPreferedDurationAndFixedDelay(Collections.singleton(task), 0, 0, 10, TimeUnit.MILLISECONDS);

        Thread.sleep(1000);

        ex.shutdown();
        
        assert executionTimes.size() > 1 : "There should have been more than 1 task executed.";

        long minDelay = 8; //10ms +- 20%
        long maxDelay = 12;
        int nofElements = executionTimes.size();
        
        long previousTime = executionTimes.poll();
        long cummulativeDiff = 0;
        while (!executionTimes.isEmpty()) {
            long thisTime = executionTimes.poll();

            long diff = thisTime - previousTime;            
            cummulativeDiff += diff;
            
            previousTime = thisTime;
        }

        long averageDelay = cummulativeDiff / (nofElements - 1);

        assert minDelay < averageDelay && averageDelay < maxDelay : "The average delay should be in <" + minDelay
            + ", " + maxDelay + "> but was " + averageDelay + ".";
    }
    
    private static BatchExecutor getExecutor(int nofThreads) {
        return new BatchExecutor(nofThreads, nofThreads, 0, TimeUnit.SECONDS);
    }
    
    private List<Callable<Void>> getCallables(final int taskDurationMillis, int nofElements) {
        List<Callable<Void>> payload = new ArrayList<Callable<Void>>();
        final Random rnd = new Random();
        final int half = taskDurationMillis / 2;
        final int fourth = half / 2;
        
        for(int i = 0; i < nofElements; ++i) {
            payload.add(new Callable<Void>() {
                
                @Override
                public Void call() throws Exception {
                    try {
                        //the tasks are going to last the taskDurationMillis +- 25%
                        int delta = rnd.nextInt(half) - fourth;
                        Thread.sleep(taskDurationMillis + delta);
                        
                        return null;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            });
        }
        
        return payload;
    }
    
    private List<Runnable> getRunnables(final int taskDurationMillis, int nofElements) {
        List<Runnable> payload = new ArrayList<Runnable>();
        final Random rnd = new Random();
        final int half = taskDurationMillis / 2;
        final int fourth = half / 2;
        
        for(int i = 0; i < nofElements; ++i) {
            payload.add(new Runnable() {
                
                @Override
                public void run() {
                    try {
                        //the tasks are going to last the taskDurationMillis +- 25%
                        int delta = rnd.nextInt(half) - fourth;
                        Thread.sleep(taskDurationMillis + delta);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        
        return payload;
    }
    
    private long rapidFireSimpleExecutorTime(final int taskDurationMillis, int nofJobs, int nofThreads) throws Exception {
        
        ThreadPoolExecutor ex = new ThreadPoolExecutor(nofThreads, nofThreads, 0, TimeUnit.NANOSECONDS, new LinkedBlockingQueue<Runnable>());
        List<Callable<Void>> payload = getCallables(taskDurationMillis, nofJobs);
        
        return measureExecutionTime(System.currentTimeMillis(), ex.invokeAll(payload));
    }    
    
    private <T> long measureExecutionTime(long startTime, List<Future<T>> futures) throws InterruptedException, ExecutionException {
        for(Future<T> f : futures) {
            f.get();
        }
        long end = System.currentTimeMillis();
        
        return end - startTime;
    }
}
