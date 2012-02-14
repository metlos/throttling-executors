/*
 * RHQ Management Platform
 * Copyright (C) 2005-2012 Red Hat, Inc.
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

package metlos.executors.ordering;

import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.DelayQueue;

/**
 * The queue behaves similarly to how the {@link DelayQueue} does - the elements from the queue are not possible
 * to poll until all the predecessors of the task are finished ({@link OrderedTask#isFinished()} return true).
 *
 * @author Lukas Krejci
 */
public class OrderedTaskQueue<E extends OrderedTask> extends AbstractQueue<E> {

    private static class ElementAndIterator<T> {
        T element;
        Iterator<T> iterator;

        public ElementAndIterator(T element, Iterator<T> iterator) {
            this.element = element;
            this.iterator = iterator;
        }
    }

    private Queue<E> elements;
    
    public OrderedTaskQueue() {
        elements = new PriorityQueue<E>();
    }
    
    public OrderedTaskQueue(Queue<E> inner) {
        elements = inner;
    }
    
    @Override
    public boolean offer(E e) {
        return elements.offer(e);
    }

    @Override
    public E poll() {
        ElementAndIterator<E> el = findFirstAvailable();
        if (el == null) {
            return null;
        } else {
            el.iterator.remove();
            return el.element;
        }
    }

    @Override
    public E peek() {
        ElementAndIterator<E> el = findFirstAvailable();
        if (el != null) {
            return el.element;
        } else {
            return null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<E> iterator() {
        return (Iterator<E>) (Iterator<?>) Arrays.asList(toArray()).iterator();
    }

    @Override
    public int size() {
        return elements.size();
    }

    private ElementAndIterator<E> findFirstAvailable() {
        Iterator<E> it = elements.iterator();

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
