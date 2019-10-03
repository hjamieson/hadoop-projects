package org.oclc.hadoop.perf.mr;

import org.apache.commons.cli.*;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.oclc.hadoop.perf.mr.inputformat.FixedSplitInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Creates a file of any size in HDFS.  Caller can specify the number of
 * mappers and the output size (in bytes) of each mapper.  This allows
 * you to put as much write-pressure on HDFS as needed for a test as
 * you collect metrics.
 * <p>
 * Note that you may need to disable UBER mode if you run less than 9
 * mappers.
 *
 * Each output record is 128 bytes long (random text).  The total
 * write load is num_maps * map_size / 128.  For example:
 * HdfsWriteTest -f demo -m 10 -b 2g
 *    results in 10 mappers that write 2GB of 128byte records each.
 */
public class HdfsWriteTest extends Configured implements Tool {
    public static final Logger LOG = LoggerFactory.getLogger(HdfsWriteTest.class);
    public static final String OPT_FILE = "f";
    public static final String OPT_SPLITS = "m";
    public static final String OPT_MAP_SIZE = "b";
    public static final String FIXED_SPLITS_NUMRECS = "fixed.splits.numrecs";
    public static final int RECORD_SIZE = 128;

    public final String JOBNAME = "xxx";

    public enum COUNTERS {LINES}

    public static void main(String[] args) throws Exception {
        System.exit(new HdfsWriteTest().run(args));
    }

    @Override
    public int run(String[] args) throws Exception {
        GenericOptionsParser gop = new GenericOptionsParser(args);
        setConf(gop.getConfiguration());
        CommandLine opts = processCmdline(gop.getRemainingArgs(), JOBNAME);
        getConf().setInt(FixedSplitInputFormat.FIXED_NUM_SPLITS, Integer.valueOf(opts.getOptionValue(OPT_SPLITS)));
        /*
         * if the args say 2G, calculate the number of records each mapper needs to generate
         */
        getConf().setLong(FIXED_SPLITS_NUMRECS, Utils.byteStringToLong(opts.getOptionValue(OPT_MAP_SIZE)) / RECORD_SIZE);

        Job job = Job.getInstance(getConf(), JOBNAME);
        job.setJarByClass(HdfsWriteTest.class);
        job.setMapperClass(EvenSplitMapper.class);
        job.setInputFormatClass(FixedSplitInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(opts.getOptionValue(OPT_FILE)));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        int rc = job.waitForCompletion(true) ? 0 : 1;

        return rc;
    }


    private static CommandLine processCmdline(String[] args, String jobname) {
        Options options = new Options();
        Option fileOpt = new Option(OPT_FILE, true, "output file");
        fileOpt.setRequired(true);
        options.addOption(fileOpt);
        Option splitsOption = new Option(OPT_SPLITS, true, "number of splits");
        splitsOption.setRequired(true);
        splitsOption.setArgName("number of mappers");
        options.addOption(splitsOption);
        Option numRecsOption = new Option(OPT_MAP_SIZE, true, "bytes output each mapper (1M, 1G, etc)");
        numRecsOption.setRequired(true);
        numRecsOption.setArgName("bytes out per mapper");
        options.addOption(numRecsOption);
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

    public static class EvenSplitMapper extends Mapper<Text, LongWritable, Text, Text> {
        Text VALUE_OUT = new Text();
        int numRecords = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numRecords = context.getConfiguration().getInt(FIXED_SPLITS_NUMRECS, 100);
        }

        @Override
        protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            // just emit 3 tones for now
            for (int i = 0; i < numRecords; i++) {
                VALUE_OUT.set(RandomStringUtils.randomAlphanumeric(RECORD_SIZE));
                context.write(key, VALUE_OUT);
            }
        }
    }

}

