/**
 * 
 */
package metlos.executors.throttling;

/**
 * @author Lukas Krejci
 * 
 */
public class OrderedRunnableTask extends AbstractOrderedTask<Runnable>
    implements Runnable {

    public OrderedRunnableTask(Runnable runnable) {
        super(runnable);
    }

    public void run() {
        try {
            getPayload().run();
        } finally {
            setFinished(true);
        }
    }
}
