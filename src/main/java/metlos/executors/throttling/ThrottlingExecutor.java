/**
 * 
 */
package metlos.executors.throttling;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 
 *
 * @author Lukas Krejci
 */
public class ThrottlingExecutor extends ThreadPoolExecutor {

    private float maximumCpuUsage;
    private ThrottlingStrategy throttlingStrategy;
    
    public ThrottlingExecutor(float maximumCpuUsage) {
        this(1, 1, maximumCpuUsage);
    }

    public ThrottlingExecutor(int initialPoolSize, int maxPoolSize, float maximumCpuUsage) {
        this(initialPoolSize, maxPoolSize, maximumCpuUsage, new DefaultThrottlingStrategy());
    }
    
    public ThrottlingExecutor(int initialPoolSize, int maxPoolSize, float maximumCpuUsage, ThreadFactory threadFactory) {
        this(initialPoolSize, maxPoolSize, maximumCpuUsage, new DefaultThrottlingStrategy(), threadFactory);
    }
    
    public ThrottlingExecutor(int initialPoolSize, int maxPoolSize, float maximumCpuUsage, ThrottlingStrategy throttlingStrategy) {
        this(initialPoolSize, maxPoolSize, maximumCpuUsage, throttlingStrategy, Executors.defaultThreadFactory());
    }
    
    public ThrottlingExecutor(int initialPoolSize, int maxPoolSize, float maximumCpuUsage, ThrottlingStrategy throttlingStrategy, ThreadFactory threadFactory) {
        this(initialPoolSize, maxPoolSize, maximumCpuUsage, throttlingStrategy, new LinkedBlockingQueue<Runnable>(), threadFactory);
    }
        
    public ThrottlingExecutor(int initialPoolSize, int maxPoolSize, float maximumCpuUsage, ThrottlingStrategy throttlingStrategy, BlockingQueue<Runnable> queue, ThreadFactory threadFactory) {
        super(initialPoolSize, maxPoolSize, 0, TimeUnit.MILLISECONDS, queue, threadFactory);
        this.maximumCpuUsage = maximumCpuUsage;
        setThrottlingStrategy(throttlingStrategy);
    }
        
    public ThrottlingExecutor(int initialPoolSize, int maxPoolSize, float maximumCpuUsage, ThrottlingStrategy throttlingStrategy, ThreadFactory threadFactory, RejectedExecutionHandler rejectedExecutionHandler) {
        this(initialPoolSize, maxPoolSize, maximumCpuUsage, throttlingStrategy, new LinkedBlockingQueue<Runnable>(), threadFactory, rejectedExecutionHandler); 
    }
    
    public ThrottlingExecutor(int initialPoolSize, int maxPoolSize, float maximumCpuUsage, ThrottlingStrategy throttlingStrategy, BlockingQueue<Runnable> queue, ThreadFactory threadFactory, RejectedExecutionHandler rejectedExecutionHandler) {
        super(initialPoolSize, maxPoolSize, 0, TimeUnit.MILLISECONDS, queue, threadFactory, rejectedExecutionHandler);
        this.maximumCpuUsage = maximumCpuUsage;
        setThrottlingStrategy(throttlingStrategy);
    }
    
    /**
     * @return the maximumCpuUsage
     */
    public float getMaximumCpuUsage() {
        return maximumCpuUsage;
    }

    /**
     * @param maximumCpuUsage the maximumCpuUsage to set
     */
    public void setMaximumCpuUsage(float maximumCpuUsage) {
        this.maximumCpuUsage = maximumCpuUsage;
    }
    
    /**
     * @return the throttlingStrategy
     */
    public ThrottlingStrategy getThrottlingStrategy() {        
        return throttlingStrategy;
    }

    /**
     * @param throttlingStrategy the throttlingStrategy to set
     */
    public void setThrottlingStrategy(ThrottlingStrategy throttlingStrategy) {
        if (this.throttlingStrategy != null) {
            this.throttlingStrategy.detach();
        }
        throttlingStrategy.attach(this);
        this.throttlingStrategy = throttlingStrategy;       
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);        
        throttlingStrategy.beforeExecute(r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        throttlingStrategy.afterExecute(r);
        super.afterExecute(r, t);
    }
}
