package org.oclc.hadoop.perf.mr;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This test reads a specific column from a table.  The column/data is unchanged by this test.  The caller
 * can specify a start/stop key to limit the size of the scan or only read from a specific region.
 */
public class ReadColumnTest extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(ReadColumnTest.class);

    @Override
    public int run(String[] args) throws Exception {

        // parse the args
        CommandLine cli = null;
        try {
            GenericOptionsParser gop = new GenericOptionsParser(HBaseConfiguration.create(), args);
            setConf(gop.getConfiguration());

            cli = new GnuParser().parse(getOpts(), gop.getRemainingArgs());
            if (cli.getOptionValue("c").split(":").length != 2) {
                throw new IllegalArgumentException("column must use family:column format");
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            new HelpFormatter().printHelp(this.getClass().getSimpleName(), getOpts());
            System.exit(1);
        }

        // log what we are running:
        LOG.info("*** Table: {}", cli.getOptionValue("t"));
        LOG.info("*** startKey: {}", cli.getOptionValue("s"));
        LOG.info("*** endKey: {}", cli.getOptionValue("e"));
        LOG.info("*** column: {}", cli.getOptionValue("c"));
        LOG.info("*** output: {}", cli.getOptionValue("o"));

        Job job = Job.getInstance(getConf(), "HBase Column Test");
        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(0);
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        if (cli.hasOption("s")) {
            scan.setStartRow(cli.getOptionValue("s").getBytes());
        }
        if (cli.hasOption("e")) {
            scan.setStopRow(cli.getOptionValue("e").getBytes());
        }
        TableMapReduceUtil.initTableMapperJob(cli.getOptionValue("t"), scan, Mapper.class, Text.class, Text.class, job);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(cli.getOptionValue("o")));
        Boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }

    /**
     * This mapper reads the row and the column we asked for and simply writes
     * out the key and a dummy value
     */
    public static class Mapper extends TableMapper<Text, Text> {
        private Text outKey = new Text();
        private Text outVal = new Text("dummy");

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            outKey.set(key.get());
            context.write(outKey, outVal);
        }
    }


    private Options getOpts() {
        Options opts = new Options();

        Option opt = new Option("t", "table", true, "table to scan");
        opt.setRequired(true);
        opts.addOption(opt);

        opts.addOption(new Option("s", "startKey", true, "starting row key"));
        opts.addOption(new Option("e", "endKey", true, "end row key"));

        opt = new Option("c", "column", true, "family:column to read");
        opt.setRequired(true);
        opts.addOption(opt);

        opt = new Option("o", "outputPath", true, "output directory");
        opt.setRequired(true);
        opts.addOption(opt);

        return opts;
    }

    public static void main(String[] args) {
        int rc = 0;
        try {
            rc = new ReadColumnTest().run(args);
        } catch (Exception e) {
            e.printStackTrace();
            rc = 1;
        }
        System.exit(rc);
    }
}
