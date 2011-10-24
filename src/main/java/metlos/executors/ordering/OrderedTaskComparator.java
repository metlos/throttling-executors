/**
 * 
 */
package metlos.executors.ordering;

import java.util.Comparator;

/**
 * @author Lukas Krejci
 * 
 */
public class OrderedTaskComparator<T extends OrderedTask> implements
    Comparator<T> {

    public static <E extends OrderedTask> int compareStatically(E o1, E o2) {
        return o1.getDepth() - o2.getDepth();
    }
    
    @Override
    public int compare(T o1, T o2) {
        return compareStatically(o1, o2);
    }
}
