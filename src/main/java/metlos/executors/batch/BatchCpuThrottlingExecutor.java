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
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 *
 * @author Lukas Krejci
 */
public class BatchCpuThrottlingExecutor extends BatchExecutor {

    private static final Log LOG = LogFactory.getLog(BatchCpuThrottlingExecutor.class);
    
    private class ThreadUsageRecord {
        public long initialCpuTime;
        public long startTime;
    }

    private ThreadLocal<ThreadUsageRecord> threadUsageRecord = new ThreadLocal<ThreadUsageRecord>();

    private ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    private final float maximumCpuUsage;
    
    private AtomicInteger currentlyExecutingTasks = new AtomicInteger();
    
    public BatchCpuThrottlingExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
        ThreadFactory threadFactory, RejectedExecutionHandler handler, float maximumCpuUsage) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, threadFactory, handler);
        this.maximumCpuUsage = maximumCpuUsage;
    }

    public BatchCpuThrottlingExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
        ThreadFactory threadFactory, float maximumCpuUsage) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, threadFactory);
        this.maximumCpuUsage = maximumCpuUsage;
    }

    public BatchCpuThrottlingExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, float maximumCpuUsage) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit);
        this.maximumCpuUsage = maximumCpuUsage;
    }

    
    protected <T extends BatchReferringRunnable<?>> BatchCpuThrottlingExecutor(int corePoolSize, int maximumPoolSize,
        long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory, RejectedExecutionHandler handler,
        TaskQueue<T> queue, float maximumCpuUsage) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, threadFactory, handler, queue);
        this.maximumCpuUsage = maximumCpuUsage;
    }

    /**
     * @return the maximumCpuUsage
     */
    public float getMaximumCpuUsage() {
        return maximumCpuUsage;
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        ThreadUsageRecord threadRecord = getThreadUsageRecord();

        if (threadRecord.startTime == 0) {
            long startTime = threadBean.getCurrentThreadCpuTime();
            threadRecord.initialCpuTime = startTime;
            threadRecord.startTime = System.nanoTime(); 
        }
    }
    
    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        currentlyExecutingTasks.decrementAndGet();
        
        ThreadUsageRecord rec = getThreadUsageRecord();

        //compute the CPU usage for the execution that just happened
        
        long startTime = rec.startTime;
        long now = System.nanoTime();
        long initialCpuTime = rec.initialCpuTime;
        long finalCpuTime = threadBean.getCurrentThreadCpuTime();
        
        long cpuTime = finalCpuTime - initialCpuTime;
        
        if (cpuTime == 0) {
            //collecting cpu time is not very accurate, so let's wait until
            //we have some data to go with
            return;
        }
        
        long duration = now - startTime;
        
        //now figure out how long to wait so that the overall CPU usage gets into
        //the limit
        
        //we know what is the allowed usage we must fit into
        float allowedUsage = getMaximumCpuUsage() / getPoolSize();
        
        //and we know an alternative expression for allowed usage:
        //allowedUsage = cpuTime / (duration + correction);
        //  ||
        //  \/
        //correction = (cpuTime - allowedUsage * duration) / allowedUsage
        
        long correction = (long) ((cpuTime - allowedUsage * duration) / allowedUsage);
        
        if (LOG.isTraceEnabled()) {
            //long durationMs = duration / 1000000;
            //long cpuTimeMs = cpuTime / 1000000;
            //long correctionMs = correction / 1000000;
            LOG.trace("Execution correction: tasks duration=" + duration + "ns, initialCpuTime=" + initialCpuTime + ", finalCpuTime=" + finalCpuTime + ", cpuTime=" + cpuTime + "ns, correction=" + correction + "ns, poolsize=" + getPoolSize() + ", allowedUsage=" + allowedUsage);
        }
        
        //reset the time collection
        rec.startTime = 0;
        
        if (correction > 0) {
            LockSupport.parkNanos(correction);
        }
    }

    private ThreadUsageRecord getThreadUsageRecord() {
        ThreadUsageRecord r = threadUsageRecord.get();
        if (r == null) {
            r = new ThreadUsageRecord();
            threadUsageRecord.set(r);
        }

        return r;
    }
}
