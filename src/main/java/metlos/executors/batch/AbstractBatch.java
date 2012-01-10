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

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;


/**
 * 
 *
 * @author Lukas Krejci
 */
public abstract class AbstractBatch<T extends Batch<T>> implements Batch<T> {

    private Batch<T> parent;
    
    protected class ParentUpdatingQueue implements Queue<T> {
        private Queue<T> inner;

        public ParentUpdatingQueue(Queue<T> q) {
            inner = q;
        }
        
        @Override
        public boolean add(T e) {
            if (inner.add(e)) {
                e.setParentBatch(AbstractBatch.this);
                return true;
            }
            return false;
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
        public boolean offer(T e) {
            if (inner.offer(e)) {
                e.setParentBatch(AbstractBatch.this);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean contains(Object o) {
            return inner.contains(o);
        }

        @Override
        public Iterator<T> iterator() {
            return inner.iterator();
        }

        @Override
        public T remove() {
            return inner.remove();
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
        public <U> U[] toArray(U[] a) {
            return inner.toArray(a);
        }

        @Override
        public boolean remove(Object o) {
            return inner.remove(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return inner.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends T> c) {
            boolean ret = false;
            
            for(T i : c) {
                ret = add(i) || ret;
            }
            
            return ret;
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
    
    protected AbstractBatch(Batch<T> parent) {
        this.parent = parent;
    }
    
    @Override
    public Batch<T> getParentBatch() {
        return parent;
    }
    
    @Override
    public void setParentBatch(Batch<T> parent) {
        this.parent = parent;
    }
}
