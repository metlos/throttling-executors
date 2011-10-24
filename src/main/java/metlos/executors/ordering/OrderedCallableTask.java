/**
 * 
 */
package metlos.executors.ordering;

import java.util.Collection;
import java.util.concurrent.Callable;

/**
 * @author Lukas Krejci
 * 
 */
public class OrderedCallableTask<V> extends AbstractOrderedTask<Callable<V>>
    implements Callable<V> {

    public OrderedCallableTask(Callable<V> callable) {
        super(callable);
    }

    public OrderedCallableTask(Callable<V> callable, OrderedTask... predecessors) {
        super(callable, predecessors);
    }
    
    public OrderedCallableTask(Callable<V> callable, Collection<? extends OrderedTask> predecessors) {
        super(callable, predecessors);
    }
    
    @Override
    public V call() throws Exception {
        try {
            return getPayload().call();
        } finally {
            setFinished(true);
        }
    }
}
