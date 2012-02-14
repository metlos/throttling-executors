package metlos.executors.ordering;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractOrderedTask<T> implements OrderedTask, Comparable<OrderedTask> {

    private final Set<OrderedTask> predecessors;
    private volatile boolean finished;
    private final T payload;
    private final int depth;

    protected AbstractOrderedTask(T payload, OrderedTask... predecessors) {
        this(payload, Arrays.asList(predecessors));
    }

    protected AbstractOrderedTask(T payload, Collection<? extends OrderedTask> predecessors) {
        this.payload = payload;
        this.predecessors = Collections.unmodifiableSet(new HashSet<OrderedTask>(predecessors));
        depth = determineDepth(this.predecessors);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof AbstractOrderedTask)) {
            return false;
        }

        AbstractOrderedTask<?> o = (AbstractOrderedTask<?>) other;

        return payload == null ? o.getPayload() == null : payload.equals(o.getPayload());
    }

    protected T getPayload() {
        return payload;
    }

    @Override
    public Set<OrderedTask> getPredecessors() {
        return predecessors;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public int getDepth() {
        return depth;
    }
    
    @Override
    public int compareTo(OrderedTask o) {
        return OrderedTaskComparator.compareStatically(this, o);
    }

    @Override
    public int hashCode() {
        return payload == null ? 0 : payload.hashCode();
    }

    @Override
    public void setFinished(boolean finished) {
        this.finished = finished;
    }

    private static int determineDepth(Set<OrderedTask> predecessors) {
        int maxParentDepth = 0;
        for (OrderedTask p : predecessors) {
            int pd = determineDepth(p.getPredecessors());
            if (pd > maxParentDepth) {
                maxParentDepth = pd;
            }
        }

        return maxParentDepth + 1;
    }
}
