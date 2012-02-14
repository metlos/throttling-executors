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

package metlos.executors.ordering.test;

import metlos.executors.ordering.AbstractOrderedTask;
import metlos.executors.ordering.OrderedTaskBlockingQueue;

import org.testng.annotations.Test;

/**
 * 
 *
 * @author Lukas Krejci
 */
@Test
public class OrderedTaskBlockingQueueTest {

    private static class Task extends AbstractOrderedTask<Object> {

        private String name;

        public Task(String name, Task... predecessors) {
            super(new Object(), predecessors);
            this.name = name;
        }

        @Override
        public void setFinished(boolean finished) {
            super.setFinished(finished);
        }

        @Override
        public String toString() {
            return "Task[" + name + ", depth=" + getDepth() + "]";
        }
    }

    public void testElementsOrderedOnPredecessors() {
        Task root1 = new Task("r1");
        Task root2 = new Task("r2");
        Task child1 = new Task("c1", root1, root2);
        Task child2 = new Task("c2", root1, root2);
        Task grandChild1 = new Task("gc1", child1);
        Task grandChild2 = new Task("gc2", child2);

        OrderedTaskBlockingQueue<Task> q = new OrderedTaskBlockingQueue<Task>();

        //add in "random" order
        q.add(grandChild1);
        q.add(grandChild2);
        q.add(root2);
        q.add(child1);
        q.add(root1);
        q.add(child2);

        Task t = q.poll();
        t.setFinished(true);
        assert t == root1 || t == root2 : "The roots should be obtained first from an ordered q";

        t = q.poll();
        t.setFinished(true);
        assert t == root1 || t == root2 : "The roots should be obtained first from an ordered q";

        t = q.poll();
        t.setFinished(true);
        assert t == child1 || t == child2 : "One of the children should be obtained second";

        t = q.poll();
        t.setFinished(true);
        assert t == child1 || t == child2 : "One of the children should be obtained second";

        t = q.poll();
        t.setFinished(true);
        assert t == grandChild1 || t == grandChild2 : "One of the grand children should be obtained last";

        t = q.poll();
        t.setFinished(true);
        assert t == grandChild1 || t == grandChild2 : "One of the grand children should be obtained last";
    }

    public void testElementsRemovedFromQueueBasedOnFinishedStatus() {
        Task root1 = new Task("r1");
        Task root2 = new Task("r2");
        Task child1 = new Task("c1", root1);
        Task child2 = new Task("c2", root1, root2);
        Task grandChild1 = new Task("gc1", child1);
        Task grandChild2 = new Task("gc2", child2);

        OrderedTaskBlockingQueue<Task> q = new OrderedTaskBlockingQueue<Task>();

        //add in "random" order
        q.add(grandChild1);
        q.add(grandChild2);
        q.add(root2);
        q.add(child1);
        q.add(root1);
        q.add(child2);

        Task t = q.poll();
        assert t == root1 || t == root2 : "The roots should be obtained first from an ordered q";

        t = q.poll();
        assert t == root1 || t == root2 : "The roots should be obtained first from an ordered q";

        t = q.poll();
        assert t == null : "No more tasks should be available until at least one of the roots finishes";

        root1.setFinished(true);

        t = q.peek();
        assert t == child1 : "Child1 should be available when only root1 finished";

        root2.setFinished(true);

        t = q.poll();
        assert t == child1 || t == child2 : "Both children should be available after roots are finished";

        t = q.poll();
        assert t == child1 || t == child2 : "Both children should be available after roots are finished";

        t = q.poll();
        assert t == null : "Grand children shouldn't be available until children are finished";

        child1.setFinished(true);

        t = q.poll();
        assert t == grandChild1 : "Granchild1 should be available after child1 finished";

        t = q.poll();
        assert t == null : "Grandchild2 shouldn't be available until child2 finishes";

        child2.setFinished(true);

        t = q.poll();
        assert t == grandChild2 : "Granchild2 should be available";
    }
}
