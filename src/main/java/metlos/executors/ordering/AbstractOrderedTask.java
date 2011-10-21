package metlos.executors.ordering;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractOrderedTask<T> implements OrderedTask {

    private Set<OrderedTask> predecessors = new HashSet<OrderedTask>();
    private boolean finished;
    private T payload;

    protected AbstractOrderedTask(T payload) {
        this.payload = payload;
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

        return payload.equals(o.getPayload());
    }

    protected T getPayload() {
        return payload;
    }

    public Set<OrderedTask> getPredecessors() {
        return predecessors;
    }

    public boolean isFinished() {
        return finished;
    }

    @Override
    public int hashCode() {
        return payload.hashCode();
    }

    protected void setFinished(boolean finished) {
        this.finished = finished;
    }
}
