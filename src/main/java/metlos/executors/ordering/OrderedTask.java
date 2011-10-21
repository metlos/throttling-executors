/**
 * 
 */
package metlos.executors.ordering;

import java.util.Set;

/**
 * @author Lukas Krejci
 */
public interface OrderedTask extends Finishable {

    Set<OrderedTask> getPredecessors();
}
