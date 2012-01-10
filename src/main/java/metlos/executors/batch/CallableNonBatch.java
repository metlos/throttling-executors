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

import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * This is an implementation of the {@link BatchedCallable} that implements the
 * non-batching behaviour. I.e. subclasses of this class are understood by the {@link BatchAwareQueue} as
 * "elements" and not batches.
 * <p>
 * The subclasses need to implement the {@link Callable#call() call()} method specified by the {@link Callable} interface. 
 *
 * @author Lukas Krejci
 */
public abstract class CallableNonBatch<V> extends AbstractBatch<BatchedCallable<V>> implements BatchedCallable<V> {

    public CallableNonBatch(BatchedCallable<V> parent) {
        super(parent);
    }
    
    /**
     * @return null so that this instance is understood as a non-batch element by the {@link BatchAwareQueue}.
     */
    @Override
    public final Queue<BatchedCallable<V>> getTasks() {
        return null;
    }
}
