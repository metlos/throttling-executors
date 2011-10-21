/**
 * 
 */
package metlos.executors.support;

/**
 * 
 *
 * @author Lukas Krejci
 */
public interface AverageComputation {
    void updateWithNextValue(float nextValue);
    float getAverageValue();
}