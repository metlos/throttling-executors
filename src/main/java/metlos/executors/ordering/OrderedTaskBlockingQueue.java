/**
 * 
 */
package metlos.executors.ordering;

import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The queue behaves similarly to how the {@link DelayQueue} does - the elements from the queue are not possible
 * to poll until all the predecessors of the task are finished ({@link OrderedTask#isFinished()} return true).
 * 
 * @author Lukas Krejci
 */
public class OrderedTaskBlockingQueue<E extends OrderedTask> extends AbstractQueue<E> implements BlockingQueue<E> {

    private static final long serialVersionUID = 1L;

    private transient final ReentrantLock lock = new ReentrantLock();
    private transient final Condition available = lock.newCondition();

    private final Queue<E> q;

    private static class ElementAndIterator<T> {
        T element;
        Iterator<T> iterator;

        public ElementAndIterator(T element, Iterator<T> iterator) {
            this.element = element;
            this.iterator = iterator;
        }
    }

    public OrderedTaskBlockingQueue() {
        this(new PriorityQueue<E>());
    }

    public OrderedTaskBlockingQueue(Queue<E> queueToWrap) {
        q = queueToWrap;
    }
    
    public OrderedTaskBlockingQueue(Collection<? extends E> c) {
        this();
        addAll(c);
    }

    public OrderedTaskBlockingQueue(int initialCapacity) {
        this(new PriorityQueue<E>(initialCapacity));
    }

    public OrderedTaskBlockingQueue(int initialCapacity, Comparator<? super E> comparator) {
        this(new PriorityQueue<E>(initialCapacity, comparator));
    }

    @Override
    public E peek() {
        lock.lock();
        try {
            ElementAndIterator<E> el = findFirstAvailable();
            if (el != null) {
                return el.element;
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E poll() {
        lock.lock();
        try {
            ElementAndIterator<E> el = findFirstAvailable();
            if (el == null) {
                return null;
            } else {
                el.iterator.remove();

                if (q.size() != 0) {
                    available.signalAll();
                }

                return el.element;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (true) {
                ElementAndIterator<E> el = findFirstAvailable();
                if (el == null) {
                    if (nanos <= 0) {
                        return null;
                    } else {
                        nanos = available.awaitNanos(nanos);
                    }
                } else {
                    el.iterator.remove();

                    if (q.size() != 0) {
                        available.signalAll();
                    }

                    return el.element;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (true) {
                ElementAndIterator<E> el = findFirstAvailable();
                if (el == null) {
                    available.await();
                } else {
                    el.iterator.remove();

                    if (q.size() != 0) {
                        available.signalAll();
                    }

                    return el.element;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(E e) {
        lock.lock();
        try {
            q.offer(e);
            available.signalAll();
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(E e) throws InterruptedException {
        offer(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(e);
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        if (c == null)
            throw new NullPointerException();
        if (c == this)
            throw new IllegalArgumentException();
        lock.lock();
        try {
            int n = 0;
            while (true) {
                ElementAndIterator<E> el = findFirstAvailable();
                if (el == null) {
                    break;
                }
                c.add(el.element);
                el.iterator.remove();
                ++n;
            }
            if (n > 0) {
                available.signalAll();
            }
            
            return n;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        if (c == null)
            throw new NullPointerException();
        if (c == this)
            throw new IllegalArgumentException();
        lock.lock();
        try {
            int n = 0;
            while (n < maxElements) {
                ElementAndIterator<E> el = findFirstAvailable();
                if (el == null) {
                    break;
                }
                c.add(el.element);
                el.iterator.remove();
                ++n;
            }
            if (n > 0) {
                available.signalAll();
            }
            
            return n;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Object[] toArray() {
        lock.lock();
        try {
            return q.toArray();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <T extends Object> T[] toArray(T[] a) {
        lock.lock();
        try {
            return q.toArray(a);
        } finally {
            lock.unlock();
        }
    };

    @Override
    public void clear() {
        lock.lock();
        try {
            q.clear();
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<E> iterator() {
        return (Iterator<E>) Arrays.asList(toArray()).iterator();
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return q.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean add(E e) {
        lock.lock();
        try {
            return q.add(e);
        } finally {
            lock.unlock();
        }
    };

    @Override
    public boolean remove(Object o) {
        lock.lock();
        try {
            return q.remove(o);
        } finally {
            lock.unlock();
        }
    }
    
    private ElementAndIterator<E> findFirstAvailable() {
        Iterator<E> it = q.iterator();

        while (it.hasNext()) {
            E element = it.next();

            //somehow the element is still in the queue even though
            //it is deemed finished. that is strange, so let's keep
            //it in the queue (if it later becomes unfinished concurrently)
            //and skip over it.
            if (element.isFinished()) {
                continue;
            }
            
            boolean allPredsFinished = true;
            for (OrderedTask pred : element.getPredecessors()) {
                if (!pred.isFinished()) {
                    allPredsFinished = false;
                    break;
                }
            }

            if (allPredsFinished) {
                return new ElementAndIterator<E>(element, it);
            }
        }

        return null;
    }
}
