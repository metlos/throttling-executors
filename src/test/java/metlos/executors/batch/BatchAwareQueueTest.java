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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.testng.annotations.Test;

/**
 * 
 *
 * @author Lukas Krejci
 */
@Test
public class BatchAwareQueueTest {

    private static class B implements Batch<B> {

        private Queue<B> tasks;
        
        public B(Queue<B> tasks) {
            this.tasks = tasks;
        }
        
        @Override
        public Queue<B> getTasks() {
            return tasks;
        }
        
    }
    
    public void testNullNotAllowed() {
        BatchAwareQueue<B> q = new BatchAwareQueue<B>();
        
        try {
            q.add(null);
            fail("Adding a null element should have failed.");
        } catch (IllegalArgumentException e) {
            
        }

        try {
            q.offer(null);
            fail("Offering a null element should have failed.");
        } catch (IllegalArgumentException e) {
            
        }
        
        try {
            q.addAll(Collections.<B>singleton(null));
            fail("Adding a null element in a collection should have failed.");
        } catch (IllegalArgumentException e) {
            
        }        
    }
    
    public void testEmptyBatchLeftOut() {
        BatchAwareQueue<B> q = new BatchAwareQueue<B>();
        
        B empty = new B(new LinkedList<B>());
        
        q.add(empty);
        
        assertEquals(q.size(), 0, "An empty batch shouldn't increase the size of the queue.");
        
        assertNull(q.poll(), "Empty batch should act as if it wasn't present in the queue at all.");
    }
    
    public void testNonBatchElementPollable() {
        BatchAwareQueue<B> q = new BatchAwareQueue<B>();
        
        B nonBatch = new B(null);
        q.add(nonBatch);
        
        assertEquals(q.size(), 1, "A non-batch should act as a single element in the queue.");
        assertSame(q.poll(), nonBatch, "Non batch should be returned as an element of the queue.");
        assertEquals(q.size(), 0, "Polling an element should decrease the size of the queue.");
    }
    
    public void testNestedBatchesHandled() {
        BatchAwareQueue<B> q = new BatchAwareQueue<B>();
        
        B el1 = new B(null);
        B el2 = new B(null);
        B el3 = new B(null);
        
        B nested = new B(new LinkedList<B>(Arrays.asList(el2, el3)));
        
        B top = new B(new LinkedList<B>(Arrays.asList(el1, nested)));
        
        q.add(top);
        
        assertEquals(q.size(), 3, "A nested queue should contribute to the count.");
        assertSame(q.poll(), el1, "Unexpected first element");
        assertEquals(q.size(), 2, "Polling an element should reduce size.");
        assertSame(q.poll(), el2, "Unexpected second element");
        assertEquals(q.size(), 1, "Polling an element should reduce size.");
        assertSame(q.poll(), el3, "Unexpected third element");
        assertEquals(q.size(), 0, "Queue should be empty.");
    }
    
    public void testIteratorTraversesBatchesRecursively() {
        BatchAwareQueue<B> q = new BatchAwareQueue<B>();
        
        B el1 = new B(null);
        B el2 = new B(null);
        B el3 = new B(null);
        
        B nested = new B(new LinkedList<B>(Arrays.asList(el1, el2)));
        
        B top = new B(new LinkedList<B>(Arrays.asList(nested, el3)));
        
        q.add(top);

        Iterator<B> it = q.iterator();
        
        assertTrue(it.hasNext(), "Iterator should see some elements");
        assertSame(it.next(), el1, "Unexpected first element");
        assertTrue(it.hasNext(), "Iterator should see some elements after first");
        assertSame(it.next(), el2, "Unexpected second element");
        assertTrue(it.hasNext(), "Iterator should see some elements after second");
        assertSame(it.next(), el3, "Unexpected third element");
        assertFalse(it.hasNext(), "Iterator should see no more elements after third");
    }
    
    public void testSizeTakesBatchNestingIntoAccount() {
        testNestedBatchesHandled();
    }
}
