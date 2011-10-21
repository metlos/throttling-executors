/**
 * 
 */
package metlos.executors.throttling;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.locks.LockSupport;

import metlos.executors.support.AverageComputation;
import metlos.executors.support.AverageComputationFactory;
import metlos.executors.support.MovingAverageFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 *
 * @author Lukas Krejci
 */
public class DefaultThrottlingStrategy implements ThrottlingStrategy {

    private static final Log LOG = LogFactory.getLog(DefaultThrottlingStrategy.class);
    
    private class ThreadUsageRecord {
        public long lastJobInitialCpuTime;
        public long lastJobStartTimeNanos;
        public long lastJobFinalCpuTime;
        public long lastJobEndTimeNanos;

        public AverageComputation averageCpuUsage;
        public AverageComputation averageDuration;

        public void updateAverages() {
            averageCpuUsage.updateWithNextValue(getLastCpuUsage());
            averageDuration.updateWithNextValue(lastJobEndTimeNanos - lastJobStartTimeNanos);
        }
        
        private float getLastCpuUsage() {
            float jobDuration = lastJobEndTimeNanos - lastJobStartTimeNanos;

            return jobDuration == 0 ? 0 : (lastJobFinalCpuTime - lastJobInitialCpuTime) / jobDuration;
        }
        
        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder("ThreadUsageRecord[");
            bld.append("avgCPU=").append(averageCpuUsage.getAverageValue() * 100f)
                .append("%,avgDuration=" + averageDuration)
                .append("ns]");
            return bld.toString();
        }
    }

    private ThreadLocal<ThreadUsageRecord> threadUsageRecord = new ThreadLocal<ThreadUsageRecord>();

    private ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    private AverageComputationFactory averageComputationFactory;
    
    private ThrottlingExecutor executor;
    
    public DefaultThrottlingStrategy() {
        this(new MovingAverageFactory(100, 100));
    }
    
    public DefaultThrottlingStrategy(AverageComputationFactory usageAverageComputationFactory) {
        this.averageComputationFactory = usageAverageComputationFactory; 
    }
    
    protected float getAverageJobCpuUsage() {
        return getThreadUsageRecord().averageCpuUsage.getAverageValue();
    }

    protected float getAverageJobDuration() {
        return getThreadUsageRecord().averageDuration.getAverageValue();
    }

    @Override
    public void attach(ThrottlingExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void detach() {
        this.executor = null;
    }
    
    @Override
    public void beforeExecute(Runnable r) {
        ThreadUsageRecord threadRecord = getThreadUsageRecord();

        System.out.println("Preparing execution in thread " + threadToString(Thread.currentThread()));
        System.out.println("Usage:" + threadRecord);
        if (LOG.isTraceEnabled()) {
            LOG.trace("Preparing execution in thread " + Thread.currentThread());
        }

        long startTime = threadBean.getCurrentThreadCpuTime();
        threadRecord.lastJobInitialCpuTime = startTime;
        threadRecord.lastJobStartTimeNanos = System.nanoTime();        
        
        long nanosToWait = determineWaitTimeNanos(Thread.currentThread(), r);
        LockSupport.parkNanos(nanosToWait);
    }

    @Override
    public void afterExecute(Runnable r) {
        ThreadUsageRecord rec = getThreadUsageRecord();

        rec.lastJobEndTimeNanos = System.nanoTime();
        long stopTime = threadBean.getCurrentThreadCpuTime();
        rec.lastJobFinalCpuTime = stopTime;

        rec.updateAverages();
    }

    private ThreadUsageRecord getThreadUsageRecord() {
        ThreadUsageRecord r = threadUsageRecord.get();
        if (r == null) {
            r = new ThreadUsageRecord();
            r.averageCpuUsage = averageComputationFactory.getNewCpuUsageAverageComputation();
            r.averageDuration = averageComputationFactory.getNewJobDurationAverageComputation();
            threadUsageRecord.set(r);
        }

        return r;
    }

    protected long determineWaitTimeNanos(Thread t, Runnable r) {
        float threadAllowance = executor.getMaximumCpuUsage() / executor.getPoolSize();
        float averageCpuUsage = getAverageJobCpuUsage();
        float averageDuration = getAverageJobDuration();

        float averageWaitTime = averageDuration - averageDuration * averageCpuUsage;
        
        float usageRatio = averageCpuUsage / threadAllowance;
        
        return (long) (averageWaitTime * usageRatio);
    }    

    private static String threadToString(Thread t) {
        return "Thread[id=0x" + Long.toHexString(t.getId()) + ", name='" + t.getName() + "']"; 
    }
}
