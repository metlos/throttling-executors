/**
 * 
 */
package metlos.executors.throttling;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Lukas Krejci
 * 
 */
public class WaitingPriorityBlockingQueue<E extends Finishable> extends
    PriorityBlockingQueue<E> {

    private static final long serialVersionUID = 1L;

    public WaitingPriorityBlockingQueue() {
    }

    public WaitingPriorityBlockingQueue(Collection<? extends E> c) {
        super(c);
    }

    public WaitingPriorityBlockingQueue(int initialCapacity) {
        super(initialCapacity);
    }

    public WaitingPriorityBlockingQueue(int initialCapacity,
        Comparator<? super E> comparator) {
        super(initialCapacity, comparator);
    }

    @Override
    public E element() {
        // TODO Auto-generated method stub
        return super.element();
    }

    @Override
    public E peek() {
        // TODO Auto-generated method stub
        return super.peek();
    }

    @Override
    public E poll() {
        // TODO Auto-generated method stub
        return super.poll();
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        // TODO Auto-generated method stub
        return super.poll(timeout, unit);
    }

    @Override
    public E take() throws InterruptedException {
        // TODO Auto-generated method stub
        return super.take();
    }

}
