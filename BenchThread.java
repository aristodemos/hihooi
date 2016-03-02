package hih;

/**
 * Created by mariosp on 28/2/16.
 */
public class BenchThread {
    private volatile boolean running = true;

    public void terminate() {
        running = false;
    }

}
