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

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper of another blocking queue implementation that is able to act as a blocking queue
 * of runnables.
 *
 * @author Lukas Krejci
 */
public class BlockingQueueWrapper<T, E extends T> implements BlockingQueue<T> {

    public interface Wrapper<X, Y> {        
        Y wrap(X object);
    }
    
    private BlockingQueue<E> inner;
    private Wrapper<T, E> wrapper;
    
    public static class CastingWrapper<X, Y> implements Wrapper<X, Y> {
        @SuppressWarnings("unchecked")
        @Override
        public Y wrap(X object) {
            return (Y) object;
        }
    }
     
    private class WrappedIterator implements Iterator<T> {
        Iterator<E> it;
        WrappedIterator(Iterator<E> it) {
            this.it = it;
        }
        
        @Override
        public boolean hasNext() {
            return it.hasNext();
        }
        
        @Override
        public T next() {
            return it.next();
        }
        
        @Override
        public void remove() {
            it.remove();
        }
    }
    
    public BlockingQueueWrapper(BlockingQueue<E> original, Wrapper<T, E> wrapper) {
        this.inner = original;
        this.wrapper = wrapper;
    }

    public BlockingQueue<E> getWrappedQueue() {
        return inner;
    }
    
    @Override
    public int size() {
        return inner.size();
    }

    @Override
    public boolean isEmpty() {
        return inner.isEmpty();
    }

    @Override
    public boolean add(T e) {
        return inner.add(wrapper.wrap(e));
    }

    @Override
    public Iterator<T> iterator() {
        return new WrappedIterator(inner.iterator());
    }

    @Override
    public T remove() {
        return inner.remove();
    }

    @Override
    public boolean offer(T e) {
        return inner.offer(wrapper.wrap(e));
    }

    @Override
    public Object[] toArray() {
        return inner.toArray();
    }

    @Override
    public T poll() {
        return inner.poll();
    }

    @Override
    public T element() {
        return inner.element();
    }

    @Override
    public T peek() {
        return inner.peek();
    }

    @Override
    public <X> X[] toArray(X[] a) {
        return inner.toArray(a);
    }

    @Override
    public void put(T e) throws InterruptedException {
        inner.put(wrapper.wrap(e));
    }

    @Override
    public boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException {
        return inner.offer(wrapper.wrap(e), timeout, unit);
    }

    @Override
    public T take() throws InterruptedException {
        return inner.take();
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return inner.poll(timeout, unit);
    }

    @Override
    public int remainingCapacity() {
        return inner.remainingCapacity();
    }

    @Override
    public boolean remove(Object o) {
        return inner.remove(o);
    }

    @Override
    public boolean contains(Object o) {
        return inner.contains(o);
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        return inner.drainTo(c);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return inner.containsAll(c);
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        return inner.drainTo(c, maxElements);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean addAll(Collection<? extends T> c) {
        return inner.addAll((Collection<? extends E>) c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return inner.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return inner.retainAll(c);
    }

    @Override
    public void clear() {
        inner.clear();
    }

    @Override
    public boolean equals(Object o) {
        return inner.equals(o);
    }

    @Override
    public int hashCode() {
        return inner.hashCode();
    }
}
