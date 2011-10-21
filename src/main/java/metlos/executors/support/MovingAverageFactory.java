/**
 * 
 */
package metlos.executors.support;

/**
 * 
 *
 * @author Lukas Krejci
 */
public class MovingAverageFactory implements AverageComputationFactory {
    
    private int cpuUsageWindowSize;
    private int jobDurationWindowSize;
    
    public MovingAverageFactory(int cpuUsageWindowSize, int jobDurationWindowSize) {
        this.cpuUsageWindowSize = cpuUsageWindowSize;
        this.jobDurationWindowSize = jobDurationWindowSize;
    }
    
    @Override
    public AverageComputation getNewCpuUsageAverageComputation() {
        return new MovingAverage(cpuUsageWindowSize);
    }
    
    @Override
    public AverageComputation getNewJobDurationAverageComputation() {
        return new MovingAverage(jobDurationWindowSize);
    }
}