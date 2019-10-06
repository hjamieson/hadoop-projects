package org.oclc.hadoop.perf.mr;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.oclc.hadoop.perf.mr.inputformat.FakeInputFormat;
import org.oclc.hadoop.perf.mr.mapper.CounterKeyMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * HBase write test that writes records to an HBase table.  There are a number
 * of facets that we would like to control:
 * - rowkey (sequential ascending, random, hashed, salted
 * - row content (single cf, multiple columns)
 * - column content (random bytes, json, xml)
 * - table geometry (splits)
 */
public class HBaseWriteTest extends Configured implements Tool {
    public static final Logger LOG = LoggerFactory.getLogger(HBaseWriteTest.class);
    public static final String OPT_TABLE = "t";
    public static final String OPT_COUNTER_TABLE = "c";
    public static final String OPT_SPLITS = "s";
    public static final String OPT_NUM_RECS = "nr";
    public final String JOBNAME = "JEDI Table Mindtrick";

    public enum COUNTERS {LINES}

    public static void main(String[] args) throws Exception {
        System.exit(new HBaseWriteTest().run(args));
    }

    @Override
    public int run(String[] args) throws Exception {
        GenericOptionsParser gop = new GenericOptionsParser(args);
        Configuration conf = gop.getConfiguration();
        conf.addResource("hbase-site.xml");
        setConf(conf);

        CommandLine opts = processCmdline(gop.getRemainingArgs(), JOBNAME);
        TableName table = TableName.valueOf(opts.getOptionValue(OPT_TABLE));

        checkTableExists(table);
        checkTableExists(TableName.valueOf(opts.getOptionValue(OPT_COUNTER_TABLE)));
        getConf().setInt(FakeInputFormat.NUM_SPLITS, Integer.valueOf(opts.getOptionValue(OPT_SPLITS)));

        getConf().setInt(CounterKeyMapper.COUNTERKEYMAPPER_NUMROWS,
                Integer.valueOf(opts.getOptionValue(OPT_NUM_RECS, "1000")));

//        Utils.locateClass(FakeInputFormat.class, getConf().getClassLoader());

        Job job = Job.getInstance(getConf(), JOBNAME);
        job.setJarByClass(HBaseWriteTest.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table.getNameWithNamespaceInclAsString());
        job.getConfiguration().set("sequence.table.name", opts.getOptionValue(OPT_COUNTER_TABLE));
        job.setMapperClass(CounterKeyMapper.class);
        job.setInputFormatClass(FakeInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Mutation.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setNumReduceTasks(0);
        TableMapReduceUtil.addDependencyJars(job);

        int rc = job.waitForCompletion(true) ? 0 : 1;

        return rc;
    }

    /**
     * make sure the table is present, or throw an IOException
     *
     * @param table
     * @throws IOException
     */
    private void checkTableExists(TableName table) throws Exception {
        try (Connection con = ConnectionFactory.createConnection(getConf());
             Admin admin = con.getAdmin()) {
            if (!admin.tableExists(table)) {
                throw new IllegalArgumentException(String.format("table %s does not exist", table.getNameWithNamespaceInclAsString()));
            }
        }
    }

    private static CommandLine processCmdline(String[] args, String jobname) {
        Options options = new Options();
        Option tableOpt = new Option(OPT_TABLE, true, "table name");
        tableOpt.setRequired(true);
        options.addOption(tableOpt);
        Option colOpt = new Option(OPT_COUNTER_TABLE, true, "sequence table");
        colOpt.setRequired(true);
        colOpt.setArgName("sequence table name");
        options.addOption(colOpt);
        Option splitOption = new Option(OPT_SPLITS, true, "number of splits");
        splitOption.setRequired(true);
        splitOption.setArgName("number of mappers");
        options.addOption(splitOption);
        options.addOption(OPT_NUM_RECS, true, "number of output recs per mapper");

        CommandLine opts = null;
        try {
            Parser cliParser = new BasicParser();
            opts = cliParser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("ERROR: " + e.getMessage());
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp(jobname, options, true);
            System.exit(1);
        }
        return opts;
    }

}
