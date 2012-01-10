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

package metlos.executors.batch;

import java.util.AbstractQueue;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * The elements of the BatchAwareQueue have a rather unusual property.
 * The type <code>E</code> extends <code>Batch&lt;E&gt;</code>. This means
 * that the type <code>E</code> is at the same time a batch of same type.
 * <p>
 * To distinguish the case where an instance of type E is <b>not</b> to be
 * considered a batch, the results{@link Batch#getTasks()} method is used. 
 * If the method returns a non-null result, the instance is considered a batch
 * otherwise it is considered a "normal" instance of type <code>E</code>. 
 * <p>
 * This might seem rather strange but in the context of executors it makes
 * much more sense if you think about the <code>E</code> as {@link Runnable}.
 * <p>
 * At that point a batch is a runnable that itself "is" a set of runnables.
 * <p>
 * This implementation of the Queue then transparently returns the constituents
 * of such batches instead of the batches themselves (i.e. returning the elements
 * from the set of runnables instead of the runnable batch itself).
 * <p>
 * Btw. this works recursively. If one of the elements of the runnable batch is
 * itself a batch, its elements are returned instead of it, recursively.
 * <p>
 * In the context of executors, this enables the batch to be executed concurrently.
 * <p>
 * This queue doesn't allow null elements.
 * 
 * @author Lukas Krejci
 */
public class BatchAwareQueue<E extends Batch<E>> extends AbstractQueue<E> {

    private static class IdentifiableIterator<T extends Batch<T>> implements Iterator<T> {
        private T batch;
        private Iterator<T> it;
        
        public IdentifiableIterator(T batch) {
            this.batch = batch;
            Queue<T> tasks = batch.getTasks();
            if (tasks != null) {
                it = tasks.iterator();
            }
        }

        @Override
        public boolean hasNext() {
            return it == null ? false : it.hasNext();
        }

        @Override
        public T next() {
            if (it == null) {
                throw new NoSuchElementException();
            }
            return it.next();
        }

        @Override
        public void remove() {
            if (it == null) {
                throw new IllegalStateException();
            }
            it.remove();
        }
        
        @Override
        public int hashCode() {
            return batch.hashCode();
        }
        
        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            
            if (!(other instanceof IdentifiableIterator)) {
                return false;
            }
            
            return batch.equals(((IdentifiableIterator<?>)other).batch);
        }
    }
    
    private static class BatchDrain<T extends Batch<T>> implements Iterator<T> {
        private Deque<IdentifiableIterator<T>> batchStack = new LinkedList<IdentifiableIterator<T>>();
        private T nextElement;
        
        public BatchDrain(T object) {
            initNext(object);
        }
        
        @Override
        public boolean hasNext() {            
            return nextElement != null;
        }
        
        @Override
        public T next() {
            T ret = nextElement;
            initNext(getNewNext());
            return ret;
        }
        
        public T peek() {
            return nextElement;
        }
        
        @Override
        public void remove() {
            batchStack.peek().remove();
        }

        public boolean isRemovable() {
            return !batchStack.isEmpty();
        }
        
        private void initNext(T toBeNext) {
            if (toBeNext == null) {
                nextElement = null;
                return;
            }
            
            Queue<T> constituents = toBeNext.getTasks();
            
            if (constituents == null) {
                nextElement = toBeNext;
            } else {
                IdentifiableIterator<T> it = new IdentifiableIterator<T>(toBeNext);
                if (it.hasNext()) {
                    if (batchStack.contains(it)) {
                        throw new IllegalArgumentException("The batch contains a loop.");
                    }
                    batchStack.push(it);
                    //XXX remove this recursion?
                    initNext(it.next());
                } else {
                    nextElement = null;
                }
            }
        }
        
        private T getNewNext() {
            while (!batchStack.isEmpty() && !batchStack.peek().hasNext()) {
                batchStack.pop();
            }
            
            return batchStack.isEmpty() ? null : batchStack.peek().next();
        }
    }
    
    public class BatchIterator implements Iterator<E> {
        private Iterator<E> elementIterator;
        private BatchDrain<E> currentDrain;
        private E nextElement;
        
        public BatchIterator() {
            elementIterator = elements.iterator();
            initNext();
        }
        
        @Override
        public boolean hasNext() {
            return nextElement != null;
        }
        
        @Override
        public E next() {
            E ret = nextElement;
            initNext();
            return ret;
        }
        
        @Override
        public void remove() {      
            if (currentDrain != null) {
                if (currentDrain.isRemovable()) {
                    currentDrain.remove();
                } else {
                    elementIterator.remove();
                }
            } else {
                throw new IllegalStateException();
            }
        }
        
        private void initNext() {
            if (currentDrain != null && currentDrain.hasNext()) {
                nextElement = currentDrain.next();
            } else {
                nextElement = null;
                currentDrain = null;
                if (elementIterator.hasNext()) {
                    currentDrain = new BatchDrain<E>(elementIterator.next());
                    //XXX remove this recursion?
                    initNext();
                }
            }
        }
    }
    
    private LinkedList<E> elements = new LinkedList<E>();
    private BatchDrain<E> currentDrain = new BatchDrain<E>(null);
    private int computedSize;
    
    @Override
    public boolean offer(E e) {
        if (e == null) {
            throw new IllegalArgumentException("Null values not allowed.");
        }
        
        if (elements.isEmpty()) {
            currentDrain = new BatchDrain<E>(e);
        }
        
        computedSize += computeSize(e);
        
        return elements.offer(e);
    }

    @Override
    public E poll() {
        if (currentDrain.hasNext()) {
            --computedSize;
            return currentDrain.next();
        } else {
            moveDrain();
            if (currentDrain.hasNext()) {
                --computedSize;
                return currentDrain.next();                
            } else {
                return null;
            }
        }
    }

    @Override
    public E peek() {
        return currentDrain.peek();
    }

    @Override
    public Iterator<E> iterator() {
        return batchIterator();
    }

    public BatchIterator batchIterator() {
        return new BatchIterator();
    }
    
    @Override
    public int size() {
        return computedSize;
    }

    @Override
    public void clear() {
        elements.clear();
    }
    
    private void moveDrain() {
        while (!elements.isEmpty()) {
            currentDrain = new BatchDrain<E>(elements.poll());
            if (currentDrain.hasNext()) {
                return;
            }
        }
        
        currentDrain = new BatchDrain<E>(null);
    }
    
    private int computeSize(E element) {
        BatchDrain<E> drain = new BatchDrain<E>(element);
        int size = 0;
        while (drain.hasNext()) {
            drain.next();
            ++size;
        }
        
        return size;
    }
}
