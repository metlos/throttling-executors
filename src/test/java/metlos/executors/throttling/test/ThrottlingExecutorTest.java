/**
 * 
 */
package metlos.executors.throttling.test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import metlos.executors.throttling.ThrottlingExecutor;

import org.testng.annotations.Test;

/**
 * 
 *
 * @author Lukas Krejci
 */
@Test
public class ThrottlingExecutorTest {

    private static class NamingThreadFactory implements ThreadFactory {
        private int count;
        
        public List<Thread> createdThreads = new ArrayList<Thread>();
        
        @Override
        public Thread newThread(Runnable r) {
            Thread ret = new Thread(r, "thread" + count++);
            ret.setDaemon(true);
            
            createdThreads.add(ret);
            
            return ret;
        }
    }
    
    private static class Payload implements Runnable {
        private Random rand = new Random();
        public void run() {
            for(int i = 0; i < rand.nextInt() % 500; ++i) {
                UUID.randomUUID();
            }
        }
    }
    
    public void testSingleThreadCpuUsage() throws Exception {
        NamingThreadFactory factory = new NamingThreadFactory();
        ThrottlingExecutor e = new ThrottlingExecutor(2, 2, .1f, factory); 
        
        List<Future<?>> payloadResults = new ArrayList<Future<?>>();
        
        long startTime = System.nanoTime();
        
        //create load
        for(int i = 0; i < 30000; ++i) {
            Future<?> f = e.submit(new Payload());
            payloadResults.add(f);
        }
        
        //wait for it all to finish
        for(Future<?> f : payloadResults) {
            f.get();
        }
        
        long endTime = System.nanoTime();
        
        //TODO figure out if it went ok
        long time = endTime - startTime;
        System.out.println("Test took " + time + "ns");
        
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        long cpuTime = 0;
        for(Thread t : factory.createdThreads) {
            long threadCpuTime = threadBean.getThreadCpuTime(t.getId());
            System.out.println(t.getName() + ": " + threadCpuTime + "ns");
            cpuTime += threadCpuTime;
        }
        
        System.out.println("Overall usage: " + ((float)cpuTime / time));
    }
    
    public void testMultipleThreadsCpuUsage() {
        
    }
}
