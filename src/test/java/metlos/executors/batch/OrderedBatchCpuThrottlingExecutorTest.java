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

package metlos.executors.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import metlos.executors.ordering.OrderedRunnableTask;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * 
 *
 * @author Lukas Krejci
 */
public class OrderedBatchCpuThrottlingExecutorTest {

    private static class OrderTrackingRunnable implements Runnable {
        private List<Integer> list;
        private int myOrder;
        
        public OrderTrackingRunnable(List<Integer> list, int myOrder) {
            this.list = list;
            this.myOrder = myOrder;
        }
        
        @Override
        public void run() {
            list.add(myOrder);
        }
    }
    
    @Test
    public void taskRunOrdered() throws Exception {
        OrderedBatchCpuThrottlingExecutor e = new OrderedBatchCpuThrottlingExecutor(5, 5, 0, TimeUnit.DAYS, 10000);
        
        final List<Integer> callOrder = Collections.synchronizedList(new ArrayList<Integer>());
        
        OrderedRunnableTask t1 = new OrderedRunnableTask(new OrderTrackingRunnable(callOrder, 1));
        OrderedRunnableTask t2 = new OrderedRunnableTask(new OrderTrackingRunnable(callOrder, 2), Collections.singleton(t1));
        OrderedRunnableTask t3 = new OrderedRunnableTask(new OrderTrackingRunnable(callOrder, 3), Collections.singleton(t2));
        OrderedRunnableTask t4 = new OrderedRunnableTask(new OrderTrackingRunnable(callOrder, 4), Collections.singleton(t3));
        OrderedRunnableTask t5 = new OrderedRunnableTask(new OrderTrackingRunnable(callOrder, 5), Collections.singleton(t4));
        
        List<Future<?>> futures = new ArrayList<Future<?>>();
        
        futures.add(e.submit(t5));
        futures.add(e.submit(t4));
        futures.add(e.submit(t3));
        futures.add(e.submit(t2));
        futures.add(e.submit(t1));
        
        for(Future<?> f : futures) {
            f.get();
        }
        
        Assert.assertEquals(callOrder.get(0), new Integer(1));
        Assert.assertEquals(callOrder.get(1), new Integer(2));
        Assert.assertEquals(callOrder.get(2), new Integer(3));
        Assert.assertEquals(callOrder.get(3), new Integer(4));
        Assert.assertEquals(callOrder.get(4), new Integer(5));
    }
}
