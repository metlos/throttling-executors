/**
 * 
 */
package metlos.executors.throttling;

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

    public V call() throws Exception {
        try {
            return getPayload().call();
        } finally {
            setFinished(true);
        }
    }
}
