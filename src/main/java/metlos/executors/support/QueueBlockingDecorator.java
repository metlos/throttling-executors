/*
 * RHQ Management Platform
 * Copyright (C) 2005-2011 Red Hat, Inc.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package metlos.executors.support;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 
 *
 * @author Lukas Krejci
 */
public class QueueBlockingDecorator<E> implements BlockingQueue<E> {
    private static final long serialVersionUID = 1L;

    private transient final ReentrantLock lock = new ReentrantLock();
    private transient final Condition available = lock.newCondition();

    private final Queue<E> q;

    public QueueBlockingDecorator(Queue<E> queue) {
        this.q = queue;
    }
    
    /**
     * Provides access to the queue decorated by this blocking decorator.
     * Use it with extreme care to avoid race conditions.
     * 
     * @return the queue decorated by this blocking decorator
     */
    protected Queue<E> getDecoratedQueue() {
        return q;
    }
    
    /**
     * Returns the lock used to lock the access to the decorated queue.
     */
    protected ReentrantLock getLock() {
        return lock;
    }

    /**
     * This condition can be waited for or signaled to coordinate
     * different threads trying to access the queue. 
     */
    protected Condition getAvailabilityCondition() {
        return available;
    }
    
    @Override
    public E element() {
        lock.lock();
        try {
            if (isEmpty()) {
                throw new NoSuchElementException();
            }
            
            return q.peek();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E peek() {
        lock.lock();
        try {
            return q.peek();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E poll() {
        lock.lock();
        try {
            E ret = q.poll();
            
            if (ret != null && !isEmpty()) {
                available.signalAll();
            }
            
            return ret;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E remove() {
        lock.lock();
        try {
            if (isEmpty()) {
                throw new NoSuchElementException();
            }
            
            return poll();
        } finally {
            lock.unlock();
        }
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
    public boolean isEmpty() {
        lock.lock();
        try {
            return q.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<E> iterator() {
        lock.lock();
        try {
            return (Iterator<E>) Arrays.asList(toArray()).iterator();
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
    public <T> T[] toArray(T[] a) {
        lock.lock();
        try {
            return q.toArray(a);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        lock.lock();
        try {
            return q.containsAll(c);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        lock.lock();
        try {
            if(q.addAll(c)) {
                available.signalAll();
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        lock.lock();
        try {
            if (q.removeAll(c)) {
                available.signalAll();
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        lock.lock();
        try {
            if (q.retainAll(c)) {
                available.signalAll();
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            q.clear();
            available.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean add(E e) {
        lock.lock();
        try {
            if (q.add(e)) {
                available.signalAll();
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(E e) {
        lock.lock();
        try {
            boolean ret = q.offer(e);
            if (ret) {
                available.signalAll();
            }
            
            return ret;
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
    public E take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while(true) {
                if (q.peek() == null && q.isEmpty()) {
                    available.await();
                } else {
                    E ret = q.poll();
                    
                    if (q.size() != 0) {
                        available.signalAll();
                    }
                    
                    return ret;
                }
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
                E el = q.poll();
                if (el == null) {
                    if (nanos <= 0) {
                        return null;
                    } else {
                        nanos = available.awaitNanos(nanos);
                    }
                } else {
                    if (q.size() != 0) {
                        available.signalAll();
                    }

                    return el;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean remove(Object o) {
        lock.lock();
        try {
            if (q.remove(o)) {
                available.signalAll();
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean contains(Object o) {
        lock.lock();
        try {
            return q.contains(o);
        } finally {
            lock.unlock();
        }
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
                E el = q.poll();
                if (el == null) {
                    break;
                }
                c.add(el);
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
                E el = q.poll();
                if (el == null) {
                    break;
                }
                c.add(el);
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
}
