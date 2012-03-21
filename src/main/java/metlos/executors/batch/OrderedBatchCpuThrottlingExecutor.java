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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import metlos.executors.ordering.OrderedTask;
import metlos.executors.ordering.OrderedTaskQueue;

/**
 * 
 *
 * @author Lukas Krejci
 */
public class OrderedBatchCpuThrottlingExecutor extends BatchCpuThrottlingExecutor {

    private static class OrderedBatchReferringRunnable<T> extends BatchReferringRunnable<T> implements OrderedTask {

        private final OrderedTask orderingProvider;
        private boolean finished;
        
        public OrderedBatchReferringRunnable(Callable<T> callable, OrderedTask orderingProvider, BatchRecord batchRecord, long idealFinishTimeNanos) {
            super(callable, batchRecord, idealFinishTimeNanos);
            this.orderingProvider = orderingProvider;
        }

        public OrderedBatchReferringRunnable(Runnable runnable, T returnValue, OrderedTask orderingProvider, BatchRecord batchRecord,
            long idealFinishTimeNanos) {
            super(runnable, returnValue, batchRecord, idealFinishTimeNanos);
            this.orderingProvider = orderingProvider; 
        }        
        
        @Override
        public int compareTo(BatchReferringRunnable<T> o) {
            if (orderingProvider == null) {
                return super.compareTo(o);
            }
            
            //cast is safe - the executor owns the queue and doesn't allow any other type
            //to get inserted into the queue
            int ret = getDepth() - ((OrderedBatchReferringRunnable<T>)o).getDepth();
            if (ret == 0) {
                ret = super.compareTo(o);
            }
            return ret;
        }

        @Override
        public Set<OrderedTask> getPredecessors() {
            return orderingProvider == null ? Collections.<OrderedTask>emptySet() : orderingProvider.getPredecessors();
        }

        @Override
        public int getDepth() {
            return orderingProvider == null ? 0 : orderingProvider.getDepth();
        }

        @Override
        public boolean isFinished() {
            return orderingProvider == null ? finished : orderingProvider.isFinished();
        }
        
        @Override
        public void setFinished(boolean value) {
            if (orderingProvider == null) {
                finished = value;
            } else {
                orderingProvider.setFinished(value);
            }
        }
        
        @Override
        public void run() {
            if (!isFinished()) {
                super.run();
            }
            setFinished(true);
        }
    }
    
    private static TaskQueue<OrderedBatchReferringRunnable<?>> getNewQueue() {
        return new TaskQueue<OrderedBatchReferringRunnable<?>>(new OrderedTaskQueue<OrderedBatchReferringRunnable<?>>());
    }
    
    public OrderedBatchCpuThrottlingExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
        float maximumCpuUsage) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, Executors.defaultThreadFactory(), DEFAULT_REJECTED_EXECUTION_HANDLER, getNewQueue(), maximumCpuUsage);
    }

    public OrderedBatchCpuThrottlingExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
        ThreadFactory threadFactory, float maximumCpuUsage) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, threadFactory, DEFAULT_REJECTED_EXECUTION_HANDLER, getNewQueue(), maximumCpuUsage);
    }

    public OrderedBatchCpuThrottlingExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
        ThreadFactory threadFactory, RejectedExecutionHandler handler, float maximumCpuUsage) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, threadFactory, handler, getNewQueue(), maximumCpuUsage);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable, BatchRecord batchRecord, long idealFinishTime) {
        OrderedTask orderingProvider = null;
        if (callable instanceof OrderedTask) {
            orderingProvider = (OrderedTask) callable;
        }
        return new OrderedBatchReferringRunnable<T>(callable, orderingProvider, batchRecord, idealFinishTime);
    }
    
    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T result, BatchRecord batchRecord, long idealFinishTime) {
        OrderedTask orderingProvider = null;
        if (runnable instanceof OrderedTask) {
            orderingProvider = (OrderedTask) runnable;
        }
        return new OrderedBatchReferringRunnable<T>(runnable, result, orderingProvider, batchRecord, idealFinishTime);
    };    
    
    @Override
    protected void prepareForNextRepetition(Collection<? extends Runnable> tasks) {
        for(Runnable r : tasks) {
            if (r instanceof OrderedTask) {
                OrderedTask ot = (OrderedTask) r;
                ot.setFinished(false);
            }
        }
    }
}
