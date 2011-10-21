/**
 * 
 */
package metlos.executors.support;

/**
 * 
 *
 * @author Lukas Krejci
 */
public interface AverageComputationFactory {
    AverageComputation getNewCpuUsageAverageComputation();
    AverageComputation getNewJobDurationAverageComputation();
}