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

package metlos.executors.batch.spreading;

import java.util.LinkedList;
import java.util.Queue;

/**
 * 
 *
 * @author Lukas Krejci
 */
public class DurationPreferringRunnableBatch extends AbstractDurationPreferringBatch<BatchedDurationPreferringRunnable> implements BatchedDurationPreferringRunnable {

    private Queue<BatchedDurationPreferringRunnable> tasks = new ParentUpdatingQueue(new LinkedList<BatchedDurationPreferringRunnable>());
    
    /**
     * @param parent
     */
    public DurationPreferringRunnableBatch(DurationPreferringBatch<BatchedDurationPreferringRunnable> parent) {
        super(parent);
    }

    @Override
    public void run() {
    }
    
    @Override
    public Queue<BatchedDurationPreferringRunnable> getTasks() {
        return tasks;
    }
    
    @Override
    public void setPreferredBatchExecutiontime(long time) {
        //just making this method public on an actual batch
        super.setPreferredBatchExecutiontime(time);
    }
}
