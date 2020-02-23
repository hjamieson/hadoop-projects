/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package simplelogger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {

    private static Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            long ts = System.currentTimeMillis();
            LOG.info("value: {}", ts);
            Thread.sleep(5000);
        }
    }
}