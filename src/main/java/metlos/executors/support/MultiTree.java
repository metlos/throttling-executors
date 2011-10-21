/**
 * 
 */
package metlos.executors.support;

import java.util.HashSet;
import java.util.Set;

import metlos.executors.ordering.OrderedTask;

/**
 * 
 *
 * @author Lukas Krejci
 */
public class MultiTree<T extends OrderedTask> {

    private static class Node<U> {
        private U task;
        
        private Set<Node<U>> children;
        private Set<Node<U>> parents;
        
        public Node(U task) {
            this.task = task;
            children = new HashSet<Node<U>>();
            parents = new HashSet<Node<U>>();
        }
        
        @Override
        public int hashCode() {
            return task.hashCode();
        }
        
        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            
            if (!(other instanceof Node)) {
                return false;
            }
            
            Node<?> o = (Node<?>) other;
            
            return task.equals(o.task);
        }
    }
    
    private Set<Node<T>> currentTops = new HashSet<Node<T>>();
    private Set<Node<T>> allNodes = new HashSet<Node<T>>();
        
    /**
     * Adds the task and all its parents to the tree.
     * 
     * @param task
     * @return true if task was added, false
     */
    public boolean add(T task) {
        Node<T> n = new Node<T>(task);
        boolean ret = allNodes.add(n);
        
        if (ret) {
            
        }
        
        return ret;
    }
}
