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
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import metlos.executors.support.QueueBlockingDecorator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This is an extension of the {@link ThreadPoolExecutor} that add 4 new methods:
 * <ul>
 * <li> {@link #invokeAllWithin(Collection, long, TimeUnit)}
 * <li> {@link #executeAllWithin(Collection, long, TimeUnit)}
 * <li> {@link #submitWithPreferedDuration(Collection, long, TimeUnit)}
 * <li> {@link #submitWithPreferedDurationAndFixedDelay(Collection, long, long, long, TimeUnit)}
 * </ul>
 * <p>
 * Those methods ensure that given collection of tasks is executed in a time as close as possible
 * to provided duration.
 * 
 * @author Lukas Krejci
 */
public class BatchExecutor extends ThreadPoolExecutor {

    private static final Log LOG = LogFactory.getLog(BatchExecutor.class);
    
    protected static final RejectedExecutionHandler DEFAULT_REJECTED_EXECUTION_HANDLER = new AbortPolicy();
    
    protected static class BatchRecord {
        AtomicInteger currentlyRunningTasks = new AtomicInteger();
        AtomicInteger elementsRan = new AtomicInteger();
        AtomicLong cumulativeExecutionTime = new AtomicLong();
        AtomicLong nextElementStartTime = new AtomicLong();
        long finishTimeNanos;
        int nofElements;
    }

    protected static class RepetitionRecord {
        Collection<? extends Runnable> tasks;
        long delayNanos;
        long durationNanos;
    }
    
    protected static class BatchReferringRunnable<T> extends FutureTask<T> implements
        Comparable<BatchReferringRunnable<T>> {
        
        protected final BatchRecord batchRecord;

        //these two are used for ordering purposes and bear no significance wrt
        //the actual time when the task gets executed.
        protected final long idealFinishTimeNanos;
        protected final long sequenceNumber;

        public BatchReferringRunnable(Callable<T> callable, BatchRecord batchRecord, long idealFinishTimeNanos) {
            super(callable);
            this.batchRecord = batchRecord;
            this.idealFinishTimeNanos = idealFinishTimeNanos;
            sequenceNumber = SEQUENCER.incrementAndGet();
        }

        public BatchReferringRunnable(Runnable runnable, T returnValue, BatchRecord batchRecord,
            long idealFinishTimeNanos) {
            super(runnable, returnValue);
            this.batchRecord = batchRecord;
            this.idealFinishTimeNanos = idealFinishTimeNanos;
            sequenceNumber = SEQUENCER.incrementAndGet();
        }

        public BatchRecord getBatchRecord() {
            return batchRecord;
        }

        @Override
        public void run() {
            long duration = 0;
            int runningTasks = 0;
            if (batchRecord != null) {
                duration = now();
                //we need to get the number of running tasks now, before we actually run our
                //payload. That is because at this very moment, this number reflects the reality
                //much better than after running the payload where we get much more variance due
                //to different durations payloads take to finish.
                //this number is important because we use it to compute the time gaps between running
                //the payloads and we need to have an exact idea about how many tasks were
                //executing concurrently at any given time.
                runningTasks = batchRecord.currentlyRunningTasks.incrementAndGet();
            }

            try {
                super.run();
            } finally {
                if (batchRecord != null) {
                    duration = now() - duration;
                    
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Task " + this + " took " + duration + "ns.");
                    }
                    
                    long executionTime = batchRecord.cumulativeExecutionTime.addAndGet(duration);
                    int elementsRan = batchRecord.elementsRan.incrementAndGet();
    
                    batchRecord.nextElementStartTime
                        .set(getNextIdealStartTime(runningTasks, executionTime, elementsRan));
                    
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Setting the next task execution time to " + batchRecord.nextElementStartTime.get());
                    }
                    
                    batchRecord.currentlyRunningTasks.decrementAndGet();
                }
            }
        }

        protected long getNextIdealStartTime(int currentlyRunningTasks, long executionTime, int elementsRan) {
            long now = now();
            long time2Go = batchRecord.finishTimeNanos - now;
            long tasks2Go = batchRecord.nofElements - elementsRan;

            double avgExecutionTime = ((double) executionTime) / elementsRan;

            //make the next task run after a longer delay if there is more than 1 of tasks 
            //running at this very moment.
            double idealExecutionTime = ((double) time2Go) / tasks2Go * currentlyRunningTasks;

            //idealExecutionTime - avgExecution is the "time gap" between the tasks.
            //note that idealExecutionTime - avgExecutionTime can be negative, which would
            //set the next element start time in the past.
            //but that's ok - it is a sign of the batch running late and will only make the
            //elements further down the line run without any delay.
            return now + (long) (idealExecutionTime - avgExecutionTime);
        }

        @Override
        public int compareTo(BatchReferringRunnable<T> o) {
            if (this == o) {
                return 0;
            } else {
                int diff = (int) (idealFinishTimeNanos - o.idealFinishTimeNanos);
                if (diff == 0) {
                    return (int) (sequenceNumber - o.sequenceNumber);
                } else {
                    return diff;
                }
            }
        }
    }

    protected class RepeatingBatchReferringRunnable extends BatchReferringRunnable<Void> {

        protected final RepetitionRecord repetitionRecord;
        
        public RepeatingBatchReferringRunnable(Runnable runnable, BatchRecord batchRecord,
            long idealFinishTimeNanos, RepetitionRecord repetitionRecord) {
            super(runnable, null, batchRecord, idealFinishTimeNanos);
            this.repetitionRecord = repetitionRecord;
        }
        
        @Override
        public void run() {
            try {
                super.run();
            } finally {
                rescheduleIfNeeded();
            }
        }
        
        protected void rescheduleIfNeeded() {
            if (batchRecord.nofElements <= batchRecord.elementsRan.get() && batchRecord.currentlyRunningTasks.get() == 0) {
                BatchExecutor.this.submitWithPreferedDurationAndFixedDelay(repetitionRecord.tasks, repetitionRecord.delayNanos, repetitionRecord.durationNanos, repetitionRecord.delayNanos, TimeUnit.NANOSECONDS);
            }
        }
    }
    
    /**
     * System.nanoTime() is not required to be positive, so let's establish
     * a base from which to start counting the time.
     * (inspired by java.util.concurrent.ScheduledThreadPoolExecutor)
     */
    private static final long EPOCH_START = System.nanoTime();

    /**
     * In a rare case when two tasks would be scheduled to be executed at exactly the same
     * time (in nanoseconds), they are going to have unique sequence numbers which
     * we can base their ordering on. This "sequencer" is used to obtain such unique
     * sequence numbers.
     */
    private static final AtomicLong SEQUENCER = new AtomicLong();

    protected static class TaskQueue<T extends BatchReferringRunnable<?>> extends QueueBlockingDecorator<T> {

        public TaskQueue() {
            this(new PriorityQueue<T>());
        }

        protected TaskQueue(Queue<T> q) {
            super(q);
        }
        
        @Override
        public T peek() {
            getLock().lock();
            try {
                T ret = getDecoratedQueue().peek();
                return getWaitingTime(ret) <= 0 ? ret : null;
            } finally {
                getLock().unlock();
            }
        }

        @Override
        public T poll() {
            getLock().lock();
            try {
                Queue<T> q = getDecoratedQueue();
                T ret = q.peek();
                
                if (ret == null) {
                    //the underlying queue contains a null;
                    return q.poll();
                }
                
                long waitTimeNanos = getWaitingTime(ret);
                if (waitTimeNanos <= 0) {
                    ret = q.poll();
                    if (!q.isEmpty()) {
                        getAvailabilityCondition().signalAll();
                    }

                    if (LOG.isTraceEnabled()) {
                        LOG.trace(" Polling task " + ret + " for execution.");
                    }
                    
                    return ret;
                } else {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Task " + ret + " not ready for execution yet.");
                    }
                    
                    return null;
                }
            } finally {
                getLock().unlock();
            }
        }

        @Override
        public T take() throws InterruptedException {
            getLock().lockInterruptibly();
            try {
                Queue<T> q = getDecoratedQueue();
                
                while(true) {                    
                    if (q.isEmpty()) {
                        getAvailabilityCondition().await();
                    } else {
                        
                        T ret = q.peek();
                        
                        if (ret == null) {
                            //the underlying queue contains a null;
                            return q.poll();
                        }
                        
                        long waitTimeNanos = getWaitingTime(ret);
                        if (waitTimeNanos <= 0) {
                            ret = q.poll();
                            if (!q.isEmpty()) {
                                getAvailabilityCondition().signalAll();
                            }
    
                            if (LOG.isTraceEnabled()) {
                                LOG.trace(" Polling task " + ret + " for execution.");
                            }
                            
                            return ret;
                        } else {
                            if (LOG.isTraceEnabled()) {
                                LOG.trace("Task " + ret + " not ready for execution yet, waiting " + waitTimeNanos + "ns.");
                            }
                            getAvailabilityCondition().awaitNanos(waitTimeNanos);
                        }
                    }
                }
            } finally {
                getLock().unlock();
            }
        }
        
        private long getWaitingTime(T element) {
            if (LOG.isTraceEnabled()) {
                String message = "Checking for ready state of " + element + ": batch is " + element.getBatchRecord() + ", ";
                if (element.getBatchRecord() != null) {
                    message += "start time is " + element.getBatchRecord().nextElementStartTime.get() + ", ";
                }
                message += "now is " + now();
                LOG.trace(message);
            }
            return element.getBatchRecord() == null ? 0 :  element.getBatchRecord().nextElementStartTime.get() - now();
        }        
    }

    @SuppressWarnings("unchecked")
    protected TaskQueue<BatchReferringRunnable<?>> getTaskQueue() {
        return (TaskQueue<BatchReferringRunnable<?>>) (BlockingQueue<?>) getQueue();
    }

    /**
     * This enables an otherwise illegal direct cast from {@link TaskQueue} to a {@link BlockingQueue}
     * of runnables. This is safe to do in constructors, because the queue is only ever going to be 
     * exclusively added to by this class, which will ensure that the runnable being inserted is in fact
     * BatchReferringRunnable.
     * 
     * @param q
     * @return
     */
    @SuppressWarnings("unchecked")
    private static BlockingQueue<Runnable> asQueueOfRunnables(TaskQueue<?> q) {
        //let's be brutal
        return (BlockingQueue<Runnable>) (BlockingQueue<?>) q;
    }

    /**
     * @param corePoolSize
     * @param maximumPoolSize
     * @param keepAliveTime
     * @param unit
     */
    public BatchExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit) {
        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, Executors.defaultThreadFactory(), DEFAULT_REJECTED_EXECUTION_HANDLER, new TaskQueue<BatchReferringRunnable<?>>());
    }

    /**
     * @param corePoolSize
     * @param maximumPoolSize
     * @param keepAliveTime
     * @param unit
     * @param threadFactory
     */
    public BatchExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
        ThreadFactory threadFactory) {
        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, threadFactory, DEFAULT_REJECTED_EXECUTION_HANDLER, new TaskQueue<BatchReferringRunnable<?>>());
    }

    /**
     * @param corePoolSize
     * @param maximumPoolSize
     * @param keepAliveTime
     * @param unit
     * @param threadFactory
     * @param handler
     */
    public BatchExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
        ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, threadFactory, handler, new TaskQueue<BatchReferringRunnable<?>>());
    }

    protected <T extends BatchReferringRunnable<?>> BatchExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory, RejectedExecutionHandler handler, TaskQueue<T> queue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, asQueueOfRunnables(queue), threadFactory,
            handler);
        init();
    }
    
    @Override
    public void execute(Runnable command) {
        Runnable r = newTaskFor(command, null);
        super.execute(r);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        RunnableFuture<T> f = newTaskFor(task);
        super.execute(f);
        return f;
    }
    
    @Override
    public Future<?> submit(Runnable task) {
        RunnableFuture<?> f = newTaskFor(task, null);
        super.execute(f);
        return f;
    }
    
    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        RunnableFuture<T> f = newTaskFor(task, result);
        super.execute(f);
        return f;
    };
    
    /**
     * This schedules the given collection of commands so that all commands are finished executing
     * before the given time. The commands are scheduled so that the overall execution time is as close
     * to the target time as possible, but the overall time can also exceed the given time if the CPU
     * is under heavy load for example.
     * <p>
     * This of course assumes that the commands are "similar" in their computational requirements, i.e.
     * that they form a "batch" of similar tasks to execute. During execution, an average execution time
     * is used to compute the delays between individual runs. This approach would fail horribly if 
     * the tasks had very different computational demands and took wildly different durations to complete.
     * <p>
     * This method is therefore different from {@link #invokeAll(Collection, long, TimeUnit)} which can
     * cause some tasks to not execute if the timeout occurs.
     *
     * @param commands the commands to execute. All of them should have similar computational requirements
     * for this method to be able to spread their execution in the given duration.
     * @param duration the duration all the tasks should execute in. This is fulfilled only on best-effort
     * basis and cannot be guaranteed.
     * @param unit the time unit of the duration
     * 
     * @return the list of futures each corresponding to the execution of a single Runnable from 
     * the collection of commands in the same sequential order. Note that the tasks are not 
     * guaranteed to have finished upon return from this method (unlike in the 
     * {@link #invokeAll(Collection)} method).
     */
    public List<Future<?>> executeAllWithin(Collection<? extends Runnable> commands, long duration, TimeUnit unit) {
        BatchRecord batchRecord = createNewBatchRecord(commands.size(), unit, duration, 0);

        //it actually is ok if this is negative - we've got so little time to execute
        //the tasks that we are late already :) - because the execution time of the tasks
        //will be in past, they will be scheduled with no further delays
        long idealFinishTime = batchRecord.nextElementStartTime.get();
        long increment = (batchRecord.finishTimeNanos - idealFinishTime) / batchRecord.nofElements;
        List<Future<?>> ret = new ArrayList<Future<?>>();
        for (Runnable command : commands) {
            RunnableFuture<?> task = newTaskFor(command, null, batchRecord, idealFinishTime);
            super.execute(task);
            ret.add(task);
            idealFinishTime += increment;
        }
        
        return ret;
    }

    /**
     * Akin to {@link #executeAllWithin(Collection, long, TimeUnit)} but doesn't collect the futures.
     * <p>
     * This method is more appropriate if you don't need to know the results of the commands or if you
     * submit a large number of them and are memory-constrained.
     * 
     * @see #executeAllWithin(Collection, long, TimeUnit)
     */
    public void submitWithPreferedDuration(Collection<? extends Runnable> commands, long duration, TimeUnit unit) {
        BatchRecord batchRecord = createNewBatchRecord(commands.size(), unit, duration, 0);

        long idealFinishTime = batchRecord.nextElementStartTime.get();
        long increment = (batchRecord.finishTimeNanos - idealFinishTime) / batchRecord.nofElements;
        for (Runnable command : commands) {
            RunnableFuture<?> task = newTaskFor(command, null, batchRecord, idealFinishTime);
            super.execute(task);
            idealFinishTime += increment;
        }
    }
    
    /**
     * Another variation on {@link #invokeAllWithin(Collection, long, TimeUnit)}. The commands
     * will be run repeatedly (forever) with each "batch" executing with given duration.
     * There will be a given delay between two consecutive executions of the command sets.
     * <p>
     * Each execution takes a snapshot of the provided collection and executes only the commands
     * present in the collection at the time of the call of this method. Once all the commands executed,
     * another snapshot of the collection is taken and the commands are rescheduled.
     * <p>
     * This means that if you intend the collection of the commands to be mutable and change over time once
     * it has been submitted to the executor, the collection <b>MUST</b> be able to handle concurrent
     * access and modification.
     * 
     * @param commands the collection of commands to repeatedly execute
     * @param initialDelay the initial delay before the execution starts (in the provided time unit)
     * @param duration the expected duration of the execution of all commands
     * @param delay the delay between two consecutive executions of the command sets
     * @param unit the time unit of the time related parameters
     */
    public void submitWithPreferedDurationAndFixedDelay(Collection<? extends Runnable> commands, long initialDelay, long duration, long delay, TimeUnit unit) {
        prepareForNextRepetition(commands);
        
        BatchRecord batchRecord = createNewBatchRecord(commands.size(), unit, duration, initialDelay);
        
        RepetitionRecord repetitionRecord = new RepetitionRecord();
        repetitionRecord.tasks = commands;
        repetitionRecord.delayNanos = unit.toNanos(delay);
        repetitionRecord.durationNanos = unit.toNanos(duration);
        
        long idealFinishTime = batchRecord.nextElementStartTime.get();
        long increment = (batchRecord.finishTimeNanos - idealFinishTime) / batchRecord.nofElements;
        for (Runnable command : commands) {
            RunnableFuture<?> task = new RepeatingBatchReferringRunnable(command, batchRecord, idealFinishTime, repetitionRecord);
            super.execute(task);
            idealFinishTime += increment;
        }
    }
        
    /**
     * Akin to {@link #executeAllWithin(Collection, long, TimeUnit)} but using Callables.
     */
    public <T> List<Future<T>>
        invokeAllWithin(Collection<? extends Callable<T>> commands, long duration, TimeUnit unit) {
        BatchRecord batchRecord = createNewBatchRecord(commands.size(), unit, duration, 0);
        
        //it actually is ok if this is negative - we've got so little time to execute
        //the tasks that we are late already :) - because the execution time of the tasks
        //will be in past, they will be scheduled with no further delays
        long increment = (batchRecord.finishTimeNanos - now()) / batchRecord.nofElements;
        long idealFinishTime = batchRecord.nextElementStartTime.get();
        List<Future<T>> ret = new ArrayList<Future<T>>();
        for (Callable<T> command : commands) {
            RunnableFuture<T> task = newTaskFor(command, batchRecord, idealFinishTime);
            super.execute(task);
            ret.add(task);
            idealFinishTime += increment;
        }

        return ret;
    }

    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable, BatchRecord batchRecord, long idealFinishTime) {
        return new BatchReferringRunnable<T>(callable, batchRecord, idealFinishTime);
    }

    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T result, BatchRecord batchRecord, long idealFinishTime) {
        return new BatchReferringRunnable<T>(runnable, result, batchRecord, idealFinishTime);
    }
    
    //these two methods are implemented for the correct interoperability with #invokeAny and other not-overriden
    //methods.
    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        return newTaskFor(callable, null, now());
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return newTaskFor(runnable, value, null, now());
    };
    
    protected static long now() {
        return System.nanoTime() - EPOCH_START;
    }

    protected static BatchRecord createNewBatchRecord(int nofElements, TimeUnit unit, long duration, long initialDelay) {
        BatchRecord batchRecord = new BatchRecord();
        batchRecord.nofElements = nofElements;
        long now = now() + unit.toNanos(initialDelay);
        batchRecord.nextElementStartTime.set(now);
        batchRecord.finishTimeNanos = now + unit.toNanos(duration);
        return batchRecord;
    }
    
    /**
     * Called during construction. This method ensures that the core threads are initialized
     * which ensures correct behavior when submitting tasks for execution.
     * <p>
     * If you override this method, make sure to call <code>super.init();</code> otherwise
     * the executor won't behave as expected.
     */
    protected void init() {
        //this is important so that all of our tasks get queued in the queue rather
        //than submitted directly. We do depend on this because the queue is actually
        //responsible for delaying the tasks until they are ready.
        prestartAllCoreThreads();
    }
    
    /**
     * If the tasks need some kind of pre-processing before they are submitted for the next repeated
     * execution, the subclasses may override this method to perform such pre-processing.
     * <p>
     * By default, this method does nothing.
     * 
     * @param tasks
     */
    protected void prepareForNextRepetition(Collection<? extends Runnable> tasks) {
        //default implementation does nothing
    }
}
