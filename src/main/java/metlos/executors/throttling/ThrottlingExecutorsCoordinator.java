/**
 * 
 */
package metlos.executors.throttling;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * 
 *
 * @author Lukas Krejci
 */
public class ThrottlingExecutorsCoordinator extends HashMap<ThrottlingExecutor, Integer> {
    //TODO basically just an ability set the overall CPU usage and 
    //a way of setting a percentage of that usage to individual executors
    //
    //This is to support setting different priorities to avail, discovery,
    //and other executors based on some possible heuristics or preference
    
    private static final long serialVersionUID = 1L;
    
    private float maximumOverallCpuUsage;
    private Map<ThrottlingExecutor, Integer> executorPriorities;
    
    public ThrottlingExecutorsCoordinator(float maximumOverallCpuUsagePercentage) {
        this(new HashMap<ThrottlingExecutor, Integer>(), maximumOverallCpuUsagePercentage);
    }
    
    public ThrottlingExecutorsCoordinator(Map<ThrottlingExecutor, Integer> prioritizedExecutors, float maximumOverallCpuUsagePercentage) {
        executorPriorities = prioritizedExecutors;
        this.maximumOverallCpuUsage = maximumOverallCpuUsagePercentage;
        
        recomputePerExecutorCpuUsage();
    }
    
    private void recomputePerExecutorCpuUsage() {
        int total = 0;
        for(Integer prio : values()) {
            total += prio;
        }
        
        for(Map.Entry<ThrottlingExecutor, Integer> entry : entrySet()) {
            entry.getKey().setMaximumCpuUsage(maximumOverallCpuUsage * entry.getValue() / total);
        }
    }

    /**
     * @return
     * @see java.util.Map#size()
     */
    public int size() {
        return executorPriorities.size();
    }
    /**
     * @return
     * @see java.util.Map#isEmpty()
     */
    public boolean isEmpty() {
        return executorPriorities.isEmpty();
    }
    /**
     * @param key
     * @return
     * @see java.util.Map#containsKey(java.lang.Object)
     */
    public boolean containsKey(Object key) {
        return executorPriorities.containsKey(key);
    }
    /**
     * @param value
     * @return
     * @see java.util.Map#containsValue(java.lang.Object)
     */
    public boolean containsValue(Object value) {
        return executorPriorities.containsValue(value);
    }
    /**
     * @param key
     * @return
     * @see java.util.Map#get(java.lang.Object)
     */
    public Integer get(Object key) {
        return executorPriorities.get(key);
    }
    /**
     * @param key
     * @param value
     * @return
     * @see java.util.Map#put(java.lang.Object, java.lang.Object)
     */
    public Integer put(ThrottlingExecutor key, Integer value) {
        Integer ret = executorPriorities.put(key, value);
        recomputePerExecutorCpuUsage();
        return ret;
    }
    /**
     * @param key
     * @return
     * @see java.util.Map#remove(java.lang.Object)
     */
    public Integer remove(Object key) {
        Integer ret = executorPriorities.remove(key);
        recomputePerExecutorCpuUsage();
        return ret;
    }
    /**
     * @param m
     * @see java.util.Map#putAll(java.util.Map)
     */
    public void putAll(Map<? extends ThrottlingExecutor, ? extends Integer> m) {
        executorPriorities.putAll(m);
        recomputePerExecutorCpuUsage();
    }
    /**
     * 
     * @see java.util.Map#clear()
     */
    public void clear() {
        executorPriorities.clear();
        recomputePerExecutorCpuUsage();
    }
    /**
     * @return
     * @see java.util.Map#keySet()
     */
    public Set<ThrottlingExecutor> keySet() {
        return Collections.unmodifiableSet(executorPriorities.keySet());
    }
    /**
     * @return
     * @see java.util.Map#values()
     */
    public Collection<Integer> values() {
        return Collections.unmodifiableCollection(executorPriorities.values());
    }
    /**
     * @return
     * @see java.util.Map#entrySet()
     */
    public Set<Entry<ThrottlingExecutor, Integer>> entrySet() {
        return Collections.unmodifiableSet(executorPriorities.entrySet());
    }
    /**
     * @param o
     * @return
     * @see java.util.Map#equals(java.lang.Object)
     */
    public boolean equals(Object o) {
        return executorPriorities.equals(o);
    }
    /**
     * @return
     * @see java.util.Map#hashCode()
     */
    public int hashCode() {
        return executorPriorities.hashCode();
    }        
}
