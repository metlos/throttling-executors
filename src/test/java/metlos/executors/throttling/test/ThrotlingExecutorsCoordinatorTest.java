/**
 * 
 */
package metlos.executors.throttling.test;

import static org.testng.Assert.*;

import metlos.executors.throttling.ThrottlingExecutor;
import metlos.executors.throttling.ThrottlingExecutorsCoordinator;

import org.testng.annotations.Test;

/**
 * 
 *
 * @author Lukas Krejci
 */
@Test
public class ThrotlingExecutorsCoordinatorTest {

    public void testProjectionsAreReadOnly() {
        //TODO implement        
    }
    
    public void testPercentageComputationFromPriorities() {
        ThrottlingExecutor e1 = new ThrottlingExecutor(0);
        ThrottlingExecutor e2 = new ThrottlingExecutor(0);
        
        ThrottlingExecutorsCoordinator c = new ThrottlingExecutorsCoordinator(1);
        
        c.put(e1, 1);
        c.put(e2, 4);
        
        //recompute the usages as ints, so that we can repeatably compare them.
        //float is too imprecise for that.
        int e1Usage = (int) (e1.getMaximumCpuUsage() * 100);
        int e2Usage = (int) (e2.getMaximumCpuUsage() * 100);
        
        assertEquals(e1Usage, 20, "Unexpected cpu usage of the first executor");
        assertEquals(e2Usage, 80, "Unexpected cpu usage of the second executor");
    }
}
