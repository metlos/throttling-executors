/**
 * 
 */
package metlos.executors.ordering;

import java.util.PriorityQueue;
import java.util.Set;

/**
 * An ordered task is something that can be in progress or finished and has predecessors.
 * Ordered tasks are used to model dependent jobs that can be executed.
 * 
 * @author Lukas Krejci
 */
public interface OrderedTask extends Comparable<OrderedTask> {

    /**
     * @return The set of tasks this task is directly dependent on.
     */
    Set<OrderedTask> getPredecessors();
    
    /**
     * Returns the depth in the task "tree" (based on the predecessors). This is used to order
     * the tasks in {@link OrderedTaskComparator}.
     * <p>
     * The standard {@link PriorityQueue} class requires total ordering of the elements which
     * would not be possible based only on the predecessors. The depth of an element gives us a good
     * measure upon which the total ordering can be based.
     * 
     * @return the depth in the task tree
     */
    int getDepth();
    
    /**
     * @return true if this task is considered finished.
     */
    boolean isFinished();    
}
