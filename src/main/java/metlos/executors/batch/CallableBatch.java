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

import java.util.LinkedList;
import java.util.Queue;

/**
 * This is a batch of callables. The {@link #call()} method is final and always returns null because
 * a this class is to be used as a batch and is therefore never directly returned from 
 * a {@link BatchAwareQueue}.
 *
 * @author Lukas Krejci
 */
public class CallableBatch<V> extends AbstractBatch<BatchedCallable<V>> implements BatchedCallable<V> {

    private final Queue<BatchedCallable<V>> queue = new ParentUpdatingQueue(new LinkedList<BatchedCallable<V>>());
    
    public CallableBatch(BatchedCallable<V> parent) {
        super(parent);
    }
    
    @Override
    public Queue<BatchedCallable<V>> getTasks() {
        return queue;
    }

    /**
     * @return null. This is a final method because the callable batch always returns non-null
     * {@link #getTasks() tasks} and therefore is always understood as a batch of tasks and not
     * a callable.
     */
    @Override
    public final V call() throws Exception {
        return null;
    }

}
