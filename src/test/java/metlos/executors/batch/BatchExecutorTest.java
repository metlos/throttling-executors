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
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
        
        long min = (long) (expectedDuration * 0.90);
        long max = (long) (expectedDuration * 1.10);
        
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
        
        long min = (long) (expectedDuration * 0.90);
        long max = (long) (expectedDuration * 1.10);
        
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
        
        long min = (long) (expectedDuration * 0.90);
        long max = (long) (expectedDuration * 1.10);
        
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
        
        long min = (long) (expectedDuration * 0.90);
        long max = (long) (expectedDuration * 1.10);
        
        LOG.info("testTimingApproachingLimitNumberOfTasks_MultiThreaded() stats: expectedDuration=" + expectedDuration + ", actualDuration=" + actualDuration + ", diff=" + (actualDuration - expectedDuration)); 
        assert actualDuration > min && actualDuration < max : "Duration should have been something between " + min + " and " + max + "ms (ideally " + expectedDuration + ") but was " + actualDuration + "ms.";
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
