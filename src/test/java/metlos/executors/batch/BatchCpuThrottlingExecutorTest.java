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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * 
 *
 * @author Lukas Krejci
 */
public class BatchCpuThrottlingExecutorTest {
    
    private static final Log LOG = LogFactory.getLog(BatchCpuThrottlingExecutorTest.class);
    
    private static class NamingThreadFactory implements ThreadFactory {
        private int count;
        
        public List<Thread> createdThreads = new ArrayList<Thread>();
        
        @Override
        public Thread newThread(Runnable r) {
            Thread ret = new Thread(r, "thread" + count++);
            ret.setDaemon(true);
            
            createdThreads.add(ret);
            
            return ret;
        }
    }
    
    private static class Payload implements Runnable {
        private Random rand = new Random();
        
        @Override
        public void run() {
            for(int i = 0; i < rand.nextInt() % 500 + 500; ++i) {
                UUID.randomUUID();
            }
        }
    }
    
    private static final int NOF_JOBS = 3000;
    private static final float MAX_USAGE = 1;
    
    @Test
    public void maxUsage_SingleThreaded() throws Exception {
        NamingThreadFactory factory = new NamingThreadFactory();
        ThreadPoolExecutor e = new ThreadPoolExecutor(1, 1, 0, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>(), factory); 
        e.prestartAllCoreThreads();
        
        List<Future<?>> payloadResults = new ArrayList<Future<?>>();
        
        long startTime = System.nanoTime();
        
        //create load
        for(int i = 0; i < NOF_JOBS; ++i) {
            Future<?> f = e.submit(new Payload());
            payloadResults.add(f);
        }
        
        //wait for it all to finish
        for(Future<?> f : payloadResults) {
            f.get();
        }
        
        long endTime = System.nanoTime();
        
        long time = endTime - startTime;
        LOG.info("MAX Singlethreaded test took " + (time / 1000.0 / 1000.0) + "ms");
        
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        long cpuTime = 0;
        for(Thread t : factory.createdThreads) {
            long threadCpuTime = threadBean.getThreadCpuTime(t.getId());
            LOG.info(t.getName() + ": " + threadCpuTime + "ns");
            cpuTime += threadCpuTime;
        }
        
        float actualUsage = (float)cpuTime / time;
        
        LOG.info("MAX Singlethreaded overall usage: " + actualUsage);
    }
    
    @Test
    public void maxUsage_MultiThreaded() throws Exception {
        NamingThreadFactory factory = new NamingThreadFactory();
        ThreadPoolExecutor e = new ThreadPoolExecutor(10, 10, 0, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>(), factory); 
        e.prestartAllCoreThreads();
        
        List<Future<?>> payloadResults = new ArrayList<Future<?>>();
        
        long startTime = System.nanoTime();
        
        //create load
        for(int i = 0; i < NOF_JOBS; ++i) {
            Future<?> f = e.submit(new Payload());
            payloadResults.add(f);
        }
        
        //wait for it all to finish
        for(Future<?> f : payloadResults) {
            f.get();
        }
        
        long endTime = System.nanoTime();
        
        long time = endTime - startTime;
        LOG.info("MAX Multithreaded test took " + (time / 1000.0 / 1000.0) + "ms");
        
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        long cpuTime = 0;
        for(Thread t : factory.createdThreads) {
            long threadCpuTime = threadBean.getThreadCpuTime(t.getId());
            LOG.info(t.getName() + ": " + threadCpuTime + "ns");
            cpuTime += threadCpuTime;
        }
        
        float actualUsage = (float)cpuTime / time;
        
        LOG.info("MAX Multithreaded overall usage: " + actualUsage);
    }
    
    @Test
    public void cpuUsageRoughlyAdheredTo_SingleThreaded() throws Exception {
        NamingThreadFactory factory = new NamingThreadFactory();

        float expectedCpuUsage = MAX_USAGE;

        BatchCpuThrottlingExecutor e = getExecutor(1, expectedCpuUsage, factory); 

        List<Future<?>> payloadResults = new ArrayList<Future<?>>();
        
        long startTime = System.nanoTime();
        
        //create load
        for(int i = 0; i < NOF_JOBS; ++i) {
            Future<?> f = e.submit(new Payload());
            payloadResults.add(f);
        }
        
        //wait for it all to finish
        for(Future<?> f : payloadResults) {
            f.get();
        }
        
        long endTime = System.nanoTime();
        
        long time = endTime - startTime;
        LOG.info("Singlethreaded test took " + (time / 1000.0 / 1000.0) + "ms");
        
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        long cpuTime = 0;
        for(Thread t : factory.createdThreads) {
            long threadCpuTime = threadBean.getThreadCpuTime(t.getId());
            LOG.info(t.getName() + ": " + threadCpuTime + "ns");
            cpuTime += threadCpuTime;
        }
        
        float actualUsage = (float)cpuTime / time;
        
        LOG.info("Singlethreaded overall usage: " + actualUsage);
        
        //this CPU throttling stuff might not be too precise, so let's give it +-20% tolerance
        float min = expectedCpuUsage * .8f;
        float max = expectedCpuUsage * 1.2f;
        
        Assert.assertTrue(min < actualUsage && actualUsage < max,  "Actual CPU usage out of expected range: (" + min + ", " + expectedCpuUsage + ", " + max + ") != " + actualUsage); 
    }
    
    @Test
    public void cpuUsageRoughlyAdheredTo_MultiThreaded() throws Exception {
        NamingThreadFactory factory = new NamingThreadFactory();
        
        float expectedCpuUsage = MAX_USAGE;
        
        BatchCpuThrottlingExecutor e = getExecutor(10, expectedCpuUsage, factory); 
        
        List<Future<?>> payloadResults = new ArrayList<Future<?>>();
        
        long startTime = System.nanoTime();
        
        //create load
        for(int i = 0; i < NOF_JOBS; ++i) {
            Future<?> f = e.submit(new Payload());
            payloadResults.add(f);
        }
        
        //wait for it all to finish
        for(Future<?> f : payloadResults) {
            f.get();
        }
        
        long endTime = System.nanoTime();
        
        long time = endTime - startTime;
        LOG.info("Multithreaded test took " + (time / 1000.0 / 1000.0) + "ms");
        
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        long cpuTime = 0;
        for(Thread t : factory.createdThreads) {
            long threadCpuTime = threadBean.getThreadCpuTime(t.getId());
            LOG.info(t.getName() + ": " + threadCpuTime + "ns");
            cpuTime += threadCpuTime;
        }
        
        float actualUsage = (float)cpuTime / time;
        
        LOG.info("Multithreaded overall usage: " + actualUsage);
        
        //this CPU throttling stuff might not be too precise, so let's give it +-20% tolerance
        float min = expectedCpuUsage * .8f;
        float max = expectedCpuUsage * 1.2f;
        
        Assert.assertTrue(min < actualUsage && actualUsage < max,  "Actual CPU usage out of expected range: (" + min + ", " + expectedCpuUsage + ", " + max + ") != " + actualUsage); 
    }
    
    private static BatchCpuThrottlingExecutor getExecutor(int nofThreads, float cpuUsage, ThreadFactory threadFactory) {
        return new BatchCpuThrottlingExecutor(nofThreads, nofThreads, 0, TimeUnit.DAYS, threadFactory, cpuUsage); 
    }
}
