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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

/**
 * 
 *
 * @author Lukas Krejci
 */
public class Benchmark {

    private static abstract class TaskSubmitter<Executor extends ThreadPoolExecutor> {
        protected Executor executor;

        abstract List<Future<Void>> submitTasks(ThreadFactory tf, Collection<? extends Callable<Void>> tasks);

        void shutdown() {
            executor.shutdownNow();
        }
    }

    private interface PayloadFactory {
        Callable<Void> createPayload();
    }

    private static class Range implements Comparable<Range> {
        public long from;
        public long to;

        public Range(long from, long to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public int compareTo(Range other) {
            if (from > other.from) {
                return 1;
            } else {
                if (from != other.from) {
                    return -1;
                } else {
                    if (to == other.to) {
                        return 0;
                    } else if (to < other.to) {
                        return -1;
                    } else {
                        return 1;
                    }
                }
            }
        }

        public boolean contains(long value) {
            return value >= from && value < to;
        }

        @Override
        public int hashCode() {
            return (int) (from * to);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof Range)) {
                return false;
            }

            Range o = (Range) other;

            return from == o.from && to == o.to;
        }

        @Override
        public String toString() {
            return "<" + from + ", " + to + ")";
        }
    }

    private static class TestResult {
        public String benchmarkName;
        public List<Long> rawMillisecondTimesPerRun;
        public List<Long> rawCpuNanoTimes;
        public SortedMap<Range, Integer> timingHistogram;
        public SortedMap<Range, Integer> cpuUsageHistogram;
        public String description;

        @Override
        public String toString() {
            StringWriter out = new StringWriter();
            PrintWriter wrt = new PrintWriter(out, true);
            wrt.println("********************************************************************************");
            wrt.print("BENCHMARK: ");
            wrt.println(benchmarkName);
            wrt.println("********************************************************************************");
            wrt.println(description);
            wrt.println();
            wrt.write("Raw timings of inidividual runs:\n");

            double averageDuration = 0;
            double averageCpuUsage = 0;
            int num = 1;
            int nofDigits = getNofDigits(rawMillisecondTimesPerRun.size());
            String formatString = "%" + nofDigits + "d : %dms (%d%% CPU)\n";
            for (int i = 0; i < rawMillisecondTimesPerRun.size(); ++i) {
                long duration = rawMillisecondTimesPerRun.get(i);
                long cpuTime = rawCpuNanoTimes.get(i);

                int cpuUsagePercent = (int) (cpuTime / 1e6 / duration * 100);
                wrt.printf(formatString, num++, duration, cpuUsagePercent);

                averageDuration += duration;
                averageCpuUsage += cpuUsagePercent;
            }

            averageDuration /= rawMillisecondTimesPerRun.size();
            averageCpuUsage /= rawMillisecondTimesPerRun.size();

            double durationStdDev = 0;
            double cpuUsageStdDev = 0;

            for (int i = 0; i < rawMillisecondTimesPerRun.size(); ++i) {
                long duration = rawMillisecondTimesPerRun.get(i);
                long cpuTime = rawCpuNanoTimes.get(i);
                int cpuUsagePercent = (int) (cpuTime / 1e6 / duration * 100);

                double durationDiff = duration - averageDuration;
                double cpuUsageDiff = cpuUsagePercent - averageCpuUsage;

                durationStdDev += durationDiff * durationDiff;
                cpuUsageStdDev += cpuUsageDiff * cpuUsageDiff;
            }

            durationStdDev /= rawMillisecondTimesPerRun.size();
            durationStdDev = Math.sqrt(durationStdDev);

            cpuUsageStdDev /= rawMillisecondTimesPerRun.size();
            cpuUsageStdDev = Math.sqrt(cpuUsageStdDev);

            wrt.printf("Duration average and std deviation: %5.2fms, %5.2fms", averageDuration, durationStdDev);
            wrt.println();
            wrt.printf("Cpu Usage average and std deviation: %5.2f%%, %5.2f%%", averageCpuUsage, cpuUsageStdDev);

            wrt.println();
            wrt.println();

            wrt.println("Timing histogram:");

            long maxFrom = Long.MIN_VALUE;
            long maxTo = Long.MIN_VALUE;
            long maxValue = Long.MIN_VALUE;

            for (Map.Entry<Range, Integer> e : timingHistogram.entrySet()) {
                if (maxFrom < e.getKey().from) {
                    maxFrom = e.getKey().from;
                }

                if (maxTo < e.getKey().to) {
                    maxTo = e.getKey().to;
                }

                if (maxValue < e.getValue()) {
                    maxValue = e.getValue();
                }
            }

            int fromDigits = getNofDigits(maxFrom);
            int toDigits = getNofDigits(maxTo);
            int valueDigits = getNofDigits(maxValue);

            formatString = "<%-" + fromDigits + "d, %" + toDigits + "d) : (%" + valueDigits + "d)";
            for (Map.Entry<Range, Integer> e : timingHistogram.entrySet()) {
                wrt.format(formatString, e.getKey().from, e.getKey().to, e.getValue());

                //represent the value as a column of stars, too - the max value is 10 stars
                int nofStars = (int) (((double) e.getValue() / maxValue) * 10);
                for (int i = 0; i < nofStars; ++i) {
                    wrt.print('*');
                }

                wrt.println();
            }

            wrt.println();

            wrt.println("CPU usage histogram:");

            maxValue = Long.MIN_VALUE;
            for (int value : cpuUsageHistogram.values()) {
                if (maxValue < value) {
                    maxValue = value;
                }
            }

            valueDigits = getNofDigits(maxValue);

            formatString = "<%2d%%, %3d%%) : (%" + valueDigits + "d)";
            for (Map.Entry<Range, Integer> e : cpuUsageHistogram.entrySet()) {
                wrt.printf(formatString, e.getKey().from, e.getKey().to, e.getValue());

                //represent the value as a column of stars, too - the max value is 10 stars
                int nofStars = (int) (((double) e.getValue() / maxValue) * 10);
                for (int i = 0; i < nofStars; ++i) {
                    wrt.print('*');
                }

                wrt.println();
            }

            String ret = out.toString();

            wrt.close();

            return ret;
        }

        private static int getNofDigits(long value) {
            if (value < 10) {
                return 1;
            } else if (value < 100) {
                return 2;
            } else {
                return (int) Math.ceil(Math.log10(value + 1));
            }
        }
    }

    private static class TestSetup<Executor extends ThreadPoolExecutor> {
        public int runs;
        public int tasksPerRun;
        public TaskSubmitter<Executor> taskSubmitter;
        public PayloadFactory payloadFactory;
        public String description;
        public int histogramBands = 20;

        private static final ThreadMXBean THREAD_BEAN = ManagementFactory.getThreadMXBean();

        private String getTestMethodName() {
            StackTraceElement[] sts = new Exception().getStackTrace();

            return sts[2].getMethodName();
        }

        public TestResult run() throws InterruptedException, ExecutionException {
            TestResult result = new TestResult();
            result.benchmarkName = getTestMethodName();
            result.rawMillisecondTimesPerRun = new ArrayList<Long>(runs);
            result.rawCpuNanoTimes = new ArrayList<Long>(runs);
            result.timingHistogram = new TreeMap<Range, Integer>();
            result.cpuUsageHistogram = new TreeMap<Range, Integer>();
            result.description = description;

            long minDuration = Long.MAX_VALUE;
            long maxDuration = Long.MIN_VALUE;

            for (int i = 0; i < runs; ++i) {
                List<Callable<Void>> payloads = new ArrayList<Callable<Void>>(tasksPerRun);
                for (int p = 0; p < tasksPerRun; ++p) {
                    payloads.add(payloadFactory.createPayload());
                }

                final List<Thread> createdThreads = new ArrayList<Thread>();

                //create our thread factory so that we can track the CPU times
                ThreadFactory tf = new ThreadFactory() {

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r);
                        createdThreads.add(t);
                        return t;
                    }

                };

                long start = System.currentTimeMillis();

                for (Future<?> f : taskSubmitter.submitTasks(tf, payloads)) {
                    f.get();
                }

                long duration = System.currentTimeMillis() - start;

                long overallCpuTime = 0;
                for (Thread t : createdThreads) {
                    long threadCpuTime = THREAD_BEAN.getThreadCpuTime(t.getId());
                    overallCpuTime += threadCpuTime;
                }

                if (minDuration > duration) {
                    minDuration = duration;
                }

                if (maxDuration < duration) {
                    maxDuration = duration;
                }

                result.rawMillisecondTimesPerRun.add(duration);
                result.rawCpuNanoTimes.add(overallCpuTime);

                taskSubmitter.shutdown();
            }

            //create the timing histogram
            int bands =
                histogramBands > result.rawMillisecondTimesPerRun.size() ? result.rawMillisecondTimesPerRun.size()
                    : histogramBands;

            long rangeStep = (long) Math.ceil((maxDuration - minDuration) / (double) bands);
            long bandStart = minDuration;
            for (int i = 0; i < bands; ++i) {
                long bandEnd = bandStart + rangeStep;
                result.timingHistogram.put(new Range(bandStart, bandEnd), 0);
                bandStart = bandEnd;
            }

            for (long duration : result.rawMillisecondTimesPerRun) {
                for (Map.Entry<Range, Integer> e : result.timingHistogram.entrySet()) {
                    Range band = e.getKey();
                    int value = e.getValue();

                    if (band.contains(duration)) {
                        e.setValue(value + 1);
                        break;
                    }
                }
            }

            //create the Cpu usage histogram
            for (int i = 0; i < 100 * Runtime.getRuntime().availableProcessors(); i += 10) {
                result.cpuUsageHistogram.put(new Range(i, i + 10), 0);
            }

            for (int i = 0; i < result.rawCpuNanoTimes.size(); ++i) {
                long cpuTime = result.rawCpuNanoTimes.get(i);
                long duration = result.rawMillisecondTimesPerRun.get(i);

                long usagePercent = (long) (cpuTime / 1e6 / duration * 100);

                for (Map.Entry<Range, Integer> e : result.cpuUsageHistogram.entrySet()) {
                    Range band = e.getKey();
                    int value = e.getValue();

                    if (band.contains(usagePercent)) {
                        e.setValue(value + 1);
                    }
                }
            }
            return result;
        }
    }

    private static class TimedPayloadFactory implements PayloadFactory {
        private long durationMillis;

        public TimedPayloadFactory(long durationMillis) {
            this.durationMillis = durationMillis;
        }

        @Override
        public Callable<Void> createPayload() {
            return new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    long endTime = System.currentTimeMillis() + durationMillis;
                    while (System.currentTimeMillis() < endTime) {
                        UUID.randomUUID();
                    }
                    return null;
                }
            };
        }
    }

    private static class StochasticTimedPayloadFactory implements PayloadFactory {
        private long minDurationMillis;
        private long maxDurationMillis;
        private Random rnd;

        public StochasticTimedPayloadFactory(long minDurationMillis, long maxDurationMillis) {
            this.minDurationMillis = minDurationMillis;
            this.maxDurationMillis = maxDurationMillis;
            rnd = new Random();
        }

        private long getRandomDuration() {
            return minDurationMillis + (rnd.nextLong() % (maxDurationMillis - minDurationMillis));
        }

        @Override
        public Callable<Void> createPayload() {
            return new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    long endTime = System.currentTimeMillis() + getRandomDuration();
                    while (System.currentTimeMillis() < endTime) {
                        UUID.randomUUID();
                    }
                    return null;
                }
            };
        }
    }

    private static final int RUNS_PER_BENCHMARK = Integer.parseInt(System.getProperty("runs-per-benchmark", "10"));
    private static final int TASKS_PER_RUN = Integer.parseInt(System.getProperty("tasks-per-run", "20"));
    private static final int NOF_THREADS = Integer.parseInt(System.getProperty("nof-threads", "1"));

    @Test
    public void benchmarkBatchExectuor_wideSpreadOfTasks_uniformTaskDuration() throws Exception {
        TestSetup<BatchExecutor> setup = new TestSetup<BatchExecutor>() {
            {
                description =
                    "Benchmark of BatchExecutor where a low number of tasks is to be executed over a long period of time, giving each task more than enough time to finish."
                        + " A task takes 50ms and a "
                        + TASKS_PER_RUN
                        + " of tasks is to be executed in the interval of 10s (giving 3 times more time than the task duration sum).";
                payloadFactory = new TimedPayloadFactory(50);

                runs = RUNS_PER_BENCHMARK;

                tasksPerRun = TASKS_PER_RUN;

                taskSubmitter = new TaskSubmitter<BatchExecutor>() {
                    @Override
                    public List<Future<Void>> submitTasks(ThreadFactory f, Collection<? extends Callable<Void>> tasks) {
                        executor = new BatchExecutor(NOF_THREADS, NOF_THREADS, 0, TimeUnit.DAYS, f);
                        return executor.invokeAllWithin(tasks, 10, TimeUnit.SECONDS);
                    }
                };
            }
        };

        TestResult result = setup.run();

        System.out.println(result);
    }

    @Test
    public void benchmarkBatchExectuor_wideSpreadOfTasks_variedTaskDuration() throws Exception {
        TestSetup<BatchExecutor> setup = new TestSetup<BatchExecutor>() {
            {
                description =
                    "Benchmark of BatchExecutor where a low number of tasks is to be executed over a long period of time, giving each task more than enough time to finish."
                        + " A task takes 50-100ms and a "
                        + TASKS_PER_RUN
                        + " of tasks is to be executed in the interval of 20s (giving 2-4 times more time than the task duration sum).";
                payloadFactory = new StochasticTimedPayloadFactory(50, 100);

                runs = RUNS_PER_BENCHMARK;

                tasksPerRun = TASKS_PER_RUN;

                taskSubmitter = new TaskSubmitter<BatchExecutor>() {
                    @Override
                    public List<Future<Void>> submitTasks(ThreadFactory f, Collection<? extends Callable<Void>> tasks) {
                        executor = new BatchExecutor(NOF_THREADS, NOF_THREADS, 0, TimeUnit.DAYS, f);
                        return executor.invokeAllWithin(tasks, 20, TimeUnit.SECONDS);
                    }
                };
            }
        };

        TestResult result = setup.run();

        System.out.println(result);
    }

    @Test
    public void benchmarkBatchExectuor_wideSpreadOfTasks_wildlyVariedTaskDuration() throws Exception {
        TestSetup<BatchExecutor> setup = new TestSetup<BatchExecutor>() {
            {
                description =
                    "Benchmark of BatchExecutor where a low number of tasks is to be executed over a long period of time, giving each task more than enough time to finish."
                        + " A task takes 50-500ms and a "
                        + TASKS_PER_RUN
                        + " of tasks is to be executed in the interval of 30s (giving .8-6 times more time than the task duration sum).";
                payloadFactory = new StochasticTimedPayloadFactory(50, 500);

                runs = RUNS_PER_BENCHMARK;

                tasksPerRun = TASKS_PER_RUN;

                taskSubmitter = new TaskSubmitter<BatchExecutor>() {
                    @Override
                    public List<Future<Void>> submitTasks(ThreadFactory f, Collection<? extends Callable<Void>> tasks) {
                        executor = new BatchExecutor(NOF_THREADS, NOF_THREADS, 0, TimeUnit.DAYS, f);
                        return executor.invokeAllWithin(tasks, 30, TimeUnit.SECONDS);
                    }
                };
            }
        };

        TestResult result = setup.run();

        System.out.println(result);
    }

    @Test
    public void benchmarkBatchExectuor_tightSpreadOfTasks_uniformTaskDuration() throws Exception {
        TestSetup<BatchExecutor> setup = new TestSetup<BatchExecutor>() {
            {
                description =
                    "Benchmark of BatchExecutor where a high number of tasks is to be executed over a short period of time, giving each task just enough time to finish."
                        + " A task takes 50ms and a "
                        + TASKS_PER_RUN
                        + " of tasks is to be executed in the interval of 5s.";
                payloadFactory = new TimedPayloadFactory(50);

                runs = RUNS_PER_BENCHMARK;

                tasksPerRun = TASKS_PER_RUN;

                taskSubmitter = new TaskSubmitter<BatchExecutor>() {
                    @Override
                    public List<Future<Void>> submitTasks(ThreadFactory f, Collection<? extends Callable<Void>> tasks) {
                        executor = new BatchExecutor(NOF_THREADS, NOF_THREADS, 0, TimeUnit.DAYS, f);
                        return executor.invokeAllWithin(tasks, 5, TimeUnit.SECONDS);
                    }
                };
            }
        };

        TestResult result = setup.run();

        System.out.println(result);
    }

    @Test
    public void benchmarkBatchExectuor_tightSpreadOfTasks_variedTaskDuration() throws Exception {
        TestSetup<BatchExecutor> setup = new TestSetup<BatchExecutor>() {
            {
                description =
                    "Benchmark of BatchExecutor where a high number of tasks is to be executed over a short period of time, giving each task just enough time to finish."
                        + " A task takes 40-60ms and a "
                        + TASKS_PER_RUN
                        + " of tasks is to be executed in the interval of 5s.";
                payloadFactory = new StochasticTimedPayloadFactory(40, 60);

                runs = RUNS_PER_BENCHMARK;

                tasksPerRun = TASKS_PER_RUN;

                taskSubmitter = new TaskSubmitter<BatchExecutor>() {
                    @Override
                    public List<Future<Void>> submitTasks(ThreadFactory f, Collection<? extends Callable<Void>> tasks) {
                        executor = new BatchExecutor(NOF_THREADS, NOF_THREADS, 0, TimeUnit.DAYS, f);
                        return executor.invokeAllWithin(tasks, 5, TimeUnit.SECONDS);
                    }
                };
            }
        };

        TestResult result = setup.run();

        System.out.println(result);
    }

    @Test
    public void benchmarkBatchExectuor_wideSpreadOfTasks_uniformTaskDuration_lowCpuUsage() throws Exception {
        TestSetup<BatchCpuThrottlingExecutor> setup = new TestSetup<BatchCpuThrottlingExecutor>() {
            {
                description =
                    "CPU is capped at 30%. Benchmark of BatchExecutor where a low number of tasks is to be executed over a long period of time, giving each task more than enough time to finish."
                        + " A task takes 50ms and a "
                        + TASKS_PER_RUN
                        + " of tasks is to be executed in the interval of 10s (giving 3 times more time than the task duration sum).";
                payloadFactory = new TimedPayloadFactory(50);

                runs = RUNS_PER_BENCHMARK;

                tasksPerRun = TASKS_PER_RUN;

                taskSubmitter = new TaskSubmitter<BatchCpuThrottlingExecutor>() {
                    @Override
                    public List<Future<Void>> submitTasks(ThreadFactory f, Collection<? extends Callable<Void>> tasks) {
                        executor = new BatchCpuThrottlingExecutor(NOF_THREADS, NOF_THREADS, 0, TimeUnit.DAYS, f, .3f);
                        return executor.invokeAllWithin(tasks, 10, TimeUnit.SECONDS);
                    }
                };
            }
        };

        TestResult result = setup.run();

        System.out.println(result);
    }

    @Test
    public void benchmarkThreadPoolExecutor_uniformTaskDuration() throws Exception {
        TestSetup<ThreadPoolExecutor> setup = new TestSetup<ThreadPoolExecutor>() {
            {
                description =
                    "This benchmark uses the Java's ThreadPoolExecutor to establish the baseline we can compare against"
                        + " when determining the overhead the batching and cpu throttling presents. Tasks take 50ms and there is "
                        + TASKS_PER_RUN + " of them per run.";
                payloadFactory = new TimedPayloadFactory(50);

                runs = RUNS_PER_BENCHMARK;

                tasksPerRun = TASKS_PER_RUN;

                taskSubmitter = new TaskSubmitter<ThreadPoolExecutor>() {
                    @Override
                    public List<Future<Void>> submitTasks(ThreadFactory f, Collection<? extends Callable<Void>> tasks) {
                        executor =
                            new ThreadPoolExecutor(NOF_THREADS, NOF_THREADS, 0, TimeUnit.DAYS,
                                new LinkedBlockingQueue<Runnable>(), f);
                        ArrayList<Future<Void>> ret = new ArrayList<Future<Void>>();
                        for (Callable<Void> task : tasks) {
                            ret.add(executor.submit(task));
                        }

                        return ret;
                    }
                };
            }
        };

        TestResult result = setup.run();

        System.out.println(result);
    }

    @Test
    public void benchmarkBatchExecutor_uniformTaskDuration_noWait() throws Exception {
        TestSetup<BatchExecutor> setup = new TestSetup<BatchExecutor>() {
            {
                description =
                    "This benchmark runs "
                        + TASKS_PER_RUN
                        + " tasks, each taking 50ms, per run. It sets the duration to 0s to run them as quickly as possible."
                        + " This is going to give us an idea about the overhead the duration tracking imposes.";
                payloadFactory = new TimedPayloadFactory(50);

                runs = RUNS_PER_BENCHMARK;

                tasksPerRun = TASKS_PER_RUN;

                taskSubmitter = new TaskSubmitter<BatchExecutor>() {
                    @Override
                    public List<Future<Void>> submitTasks(ThreadFactory f, Collection<? extends Callable<Void>> tasks) {
                        executor = new BatchExecutor(NOF_THREADS, NOF_THREADS, 0, TimeUnit.DAYS, f);
                        List<Future<Void>> ret = executor.invokeAllWithin(tasks, 0, TimeUnit.SECONDS);
                        return ret;
                    }
                };
            }
        };

        TestResult result = setup.run();

        System.out.println(result);
    }

    @Test
    public void benchmarkBatchCpuThrottlingExecutor_uniformTaskDuration_noWait_noCpuCap() throws Exception {
        TestSetup<BatchCpuThrottlingExecutor> setup = new TestSetup<BatchCpuThrottlingExecutor>() {
            {
                description =
                    "This benchmark runs "
                        + TASKS_PER_RUN
                        + " tasks, each taking 50ms, per run. It sets the duration to 0s to run them as quickly as possible."
                        + " This is going to give us an idea about the overhead the duration tracking imposes.";
                payloadFactory = new TimedPayloadFactory(50);

                runs = RUNS_PER_BENCHMARK;

                tasksPerRun = TASKS_PER_RUN;

                taskSubmitter = new TaskSubmitter<BatchCpuThrottlingExecutor>() {
                    @Override
                    public List<Future<Void>> submitTasks(ThreadFactory f, Collection<? extends Callable<Void>> tasks) {
                        //1000 cores you say?
                        executor = new BatchCpuThrottlingExecutor(NOF_THREADS, NOF_THREADS, 0, TimeUnit.DAYS, f, 1000);
                        List<Future<Void>> ret = executor.invokeAllWithin(tasks, 0, TimeUnit.SECONDS);
                        return ret;
                    }
                };
            }
        };

        TestResult result = setup.run();

        System.out.println(result);
    }

    @Test
    public void benchmarkBatchCpuThrottlingExecutor_uniformTaskDuration_noWait_cpuCap() throws Exception {
        TestSetup<BatchCpuThrottlingExecutor> setup = new TestSetup<BatchCpuThrottlingExecutor>() {
            {
                description =
                    "This benchmark runs "
                        + TASKS_PER_RUN
                        + " tasks, each taking 50ms, per run. It sets the duration to 0s to run them as quickly as possible."
                        + " The CPU is capped at 5%. We should therefore see minimal variance in CPU usage.";
                payloadFactory = new TimedPayloadFactory(50);

                runs = RUNS_PER_BENCHMARK;

                tasksPerRun = TASKS_PER_RUN;

                taskSubmitter = new TaskSubmitter<BatchCpuThrottlingExecutor>() {
                    @Override
                    public List<Future<Void>> submitTasks(ThreadFactory f, Collection<? extends Callable<Void>> tasks) {
                        //1000 cores you say?
                        executor = new BatchCpuThrottlingExecutor(NOF_THREADS, NOF_THREADS, 0, TimeUnit.DAYS, f, .05f);
                        List<Future<Void>> ret = executor.invokeAllWithin(tasks, 0, TimeUnit.SECONDS);
                        return ret;
                    }
                };
            }
        };

        TestResult result = setup.run();

        System.out.println(result);
    }

    @Test
    public void benchmarkBatchExectuor_tightSpreadOfTasks_variedTaskDuration_cpuCap() throws Exception {
        TestSetup<BatchExecutor> setup = new TestSetup<BatchExecutor>() {
            {
                description =
                    "Benchmark of BatchExecutor where a high number of tasks is to be executed over a short period of time, giving each task just enough time to finish."
                        + " A task takes 40-60ms and a "
                        + TASKS_PER_RUN
                        + " of tasks is to be executed in the interval of 2s. The CPU is capped at 5%."
                        + " We should therefore see a violation of the time constraints but CPU cap should be enforced.";
                payloadFactory = new StochasticTimedPayloadFactory(40, 60);

                runs = RUNS_PER_BENCHMARK;

                tasksPerRun = TASKS_PER_RUN;

                taskSubmitter = new TaskSubmitter<BatchExecutor>() {
                    @Override
                    public List<Future<Void>> submitTasks(ThreadFactory f, Collection<? extends Callable<Void>> tasks) {
                        executor = new BatchCpuThrottlingExecutor(NOF_THREADS, NOF_THREADS, 0, TimeUnit.DAYS, f, .05f);
                        return executor.invokeAllWithin(tasks, 2, TimeUnit.SECONDS);
                    }
                };
            }
        };

        TestResult result = setup.run();

        System.out.println(result);
    }

        public static void main(String[] args) throws Exception {
            Benchmark me = new Benchmark();
            me.benchmarkBatchExectuor_wideSpreadOfTasks_uniformTaskDuration_lowCpuUsage();
        }
}
