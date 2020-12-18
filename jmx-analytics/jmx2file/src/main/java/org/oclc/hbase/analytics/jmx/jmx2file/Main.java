package org.oclc.hbase.analytics.jmx.jmx2file;

import org.apache.commons.cli.*;
import org.oclc.hbase.analytics.jmx.collector.JmxCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * collections JMX data from rs and writes it to a local file.
 */
public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    public static final String DEFAULT_CYCLE_SECS = "30";
    public static final int IDLE_SLEEP_TIME = 30;

    public static void main(String[] args) {
        CommandLine cli = processOptions(args);

        JmxCollector collector = new JmxCollector()
                .writeTo(new FileSink(new File(cli.getArgs()[0])))
                .withCycleSeconds(Integer.parseInt(cli.getOptionValue("c", DEFAULT_CYCLE_SECS)))
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("in shutdown hook");
            collector.shutdown();
        }));

        while (true) {
            try {
                TimeUnit.SECONDS.sleep(IDLE_SLEEP_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private static CommandLine processOptions(String[] args) throws IllegalArgumentException {
        Options options = new Options();
        Option cycle = new Option("c", "cycle", true, "cycle time in secs");
        cycle.setRequired(false);
        options.addOption(cycle);
        CommandLineParser parser = new BasicParser();
        CommandLine cli = null;
        try {
            cli = parser.parse(options, args);
        } catch (ParseException e) {
            new HelpFormatter().printHelp("options:", options);
            throw new IllegalArgumentException(e.getMessage());
        }
        return cli;
    }
}
