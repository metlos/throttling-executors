/**
 * 
 */
package metlos.executors.throttling;

import java.util.Comparator;

/**
 * @author Lukas Krejci
 * 
 */
public class OrderedTaskComparator<T extends OrderedTask> implements
    Comparator<T> {

    public int compare(T o1, T o2) {
        if (o1.getPredecessors().contains(o2)) {
            return 1;
        } else if (o2.getPredecessors().contains(o1)) {
            return -1;
        } else {
            return 0;
        }
    }

}
