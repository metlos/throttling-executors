/**
 * 
 */
package metlos.executors.support;


/**
 * 
 *
 * @author Lukas Krejci
 */
public class MovingAverage implements AverageComputation {
    private int windowSize;
    private float lastWindowAverage = Float.NaN;
    private float currentWindowAverage;
    private int currentWindowSize;

    private float currentValue;
    
    public MovingAverage(int windowSize) {
        this.windowSize = windowSize;
        this.currentWindowSize = windowSize;
    }

    public void updateWithNextValue(float nextValue) {
        if (currentWindowSize == windowSize) {
            lastWindowAverage = currentWindowAverage;
            currentWindowAverage = nextValue;
            currentWindowSize = 1;
        } else {
            //x(n) = x(n-1) - x(n-1)/n + c/n
            ++currentWindowSize;                
            currentWindowAverage = currentWindowAverage + (nextValue - currentWindowAverage) / currentWindowSize;
        }
        
        if (lastWindowAverage == Float.NaN) {
            currentValue = currentWindowAverage / currentWindowSize;
        } else {

            //this is a weighted average of the last window average and the current window average
            //weighted by their window size, i.e. the current window isn't "important" for determining
            //the average when it's new..
            currentValue = (lastWindowAverage * (windowSize - currentWindowSize) + currentWindowAverage * currentWindowSize)
                / windowSize;
        }
    }
    
    public float getAverageValue() {
        return currentValue;
    }
    
    @Override
    public String toString() {
        return Float.toString(currentValue);
    }        
}