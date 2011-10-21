/**
 * 
 */
package metlos.executors.throttling;

import java.util.Set;

/**
 * @author Lukas Krejci
 */
public interface OrderedTask extends Finishable {

    Set<OrderedTask> getPredecessors();
}
