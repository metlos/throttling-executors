/**
 * 
 */
package metlos.executors.ordering;

import java.util.concurrent.DelayQueue;

import metlos.executors.support.QueueBlockingDecorator;

/**
 * The queue behaves similarly to how the {@link DelayQueue} does - the elements from the queue are not possible
 * to poll until all the predecessors of the task are finished ({@link OrderedTask#isFinished()} return true).
 * 
 * @author Lukas Krejci
 */
public class OrderedTaskBlockingQueue<E extends OrderedTask> extends QueueBlockingDecorator<E> {

    private static final long serialVersionUID = 1L;

    public OrderedTaskBlockingQueue() {
        super(new OrderedTaskQueue<E>());
    }
    
    public OrderedTaskBlockingQueue(OrderedTaskQueue<E> q) {
        super(q);
    }
}
