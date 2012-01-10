/*
 * RHQ Management Platform
 * Copyright (C) 2005-2011 Red Hat, Inc.
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

package metlos.executors.batch.spreading;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 
 *
 * @author Lukas Krejci
 */
public class BatchTimeSpreadingExecutor extends ThreadPoolExecutor {

    @SuppressWarnings("unchecked")
    private static BlockingQueue<Runnable> castToRunnable(BlockingQueue<BatchedDurationPreferringRunnable> q) {
        return (BlockingQueue<Runnable>) (BlockingQueue<?>) q;
    }

    /**
     * @param corePoolSize
     * @param maximumPoolSize
     * @param keepAliveTime
     * @param unit
     * @param workQueue
     */
    public BatchTimeSpreadingExecutor(
        int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<BatchedDurationPreferringRunnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, castToRunnable(workQueue));
    }

    /**
     * @param corePoolSize
     * @param maximumPoolSize
     * @param keepAliveTime
     * @param unit
     * @param workQueue
     * @param threadFactory
     */
    public BatchTimeSpreadingExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
        BlockingQueue<BatchedDurationPreferringRunnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, castToRunnable(workQueue), threadFactory);
    }

    /**
     * @param corePoolSize
     * @param maximumPoolSize
     * @param keepAliveTime
     * @param unit
     * @param workQueue
     * @param handler
     */
    public BatchTimeSpreadingExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
        BlockingQueue<BatchedDurationPreferringRunnable> workQueue, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, castToRunnable(workQueue), handler);
    }

    /**
     * @param corePoolSize
     * @param maximumPoolSize
     * @param keepAliveTime
     * @param unit
     * @param workQueue
     * @param threadFactory
     * @param handler
     */
    public BatchTimeSpreadingExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
        BlockingQueue<BatchedDurationPreferringRunnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, castToRunnable(workQueue), threadFactory, handler);
    }
    
    @SuppressWarnings("unchecked")
    public BlockingQueue<BatchedDurationPreferringRunnable> getBatchedQueue() {
        //this is safe, because we guard the type to be castable in the constructors
        return (BlockingQueue<BatchedDurationPreferringRunnable>) (BlockingQueue<?>) super.getQueue();
    }
}
