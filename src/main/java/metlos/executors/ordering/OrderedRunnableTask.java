/**
 * 
 */
package metlos.executors.ordering;

import java.util.Collection;

/**
 * @author Lukas Krejci
 * 
 */
public class OrderedRunnableTask extends AbstractOrderedTask<Runnable>
    implements Runnable {

    public OrderedRunnableTask(Runnable runnable) {
        super(runnable);
    }

    public OrderedRunnableTask(Runnable runnable, OrderedTask... predecessors) {
        super(runnable, predecessors);
    }
    
    public OrderedRunnableTask(Runnable runnable, Collection<? extends OrderedTask> predecessors) {
        super(runnable, predecessors);
    }
    
    @Override
    public void run() {
        try {
            getPayload().run();
        } finally {
            setFinished(true);
        }
    }
}
