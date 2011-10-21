/**
 * 
 */
package metlos.executors.throttling;

/**
 * Implementations of this interface can affect the CPU time throttling in a {@link ThrottlingExecutor}.
 *
 * @author Lukas Krejci
 */
public interface ThrottlingStrategy {

    /**
     * This method is called when the throttling strategy is associated with the provided
     * throttling executor.
     * 
     * @param executor
     */
    void attach(ThrottlingExecutor executor);
    
    /**
     * Detaches from the executor the strategy has been previously {@link #attach(ThrottlingExecutor)}'ed to.
     */
    void detach();
    
    /**
     * This method is called right before the provided runnable is executed in the executor.
     * This method is called from the thread that will be executing the runnable <code>r</code>.
     * <p>
     * To implement CPU throttling this method should suspend the execution of the current thread
     * for the appropriate amount of time.
     * 
     * @param r the runnable to be executed
     */
    void beforeExecute(Runnable r);
    
    /**
     * This method is called right after the runnable <code>r</code> finished execution in the executor.
     * The implementation is free to perform any book-keeping it needs.
     * <p>
     * This method is executed from within the thread that executed the runnable.
     * 
     * @param r the runnable that just ran
     */
    void afterExecute(Runnable r);
}
