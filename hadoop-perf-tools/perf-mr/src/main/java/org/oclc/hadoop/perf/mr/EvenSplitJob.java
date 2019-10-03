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
import java.util.regex.Pattern;

public class EvenSplitJob extends Configured implements Tool {
    public static final Logger LOG = LoggerFactory.getLogger(EvenSplitJob.class);
    public static final String OPT_FILE = "f";
    public static final String OPT_SPLITS = "m";
    public static final String OPT_NUMRECS = "n";
    public static final String FIXED_SPLITS_NUMRECS = "fixed.splits.numrecs";
    public static final int RECORD_SIZE = 100;

    public final String JOBNAME = "xxx";

    public enum COUNTERS {LINES}

    public static void main(String[] args) throws Exception {
        System.exit(new EvenSplitJob().run(args));
    }

    @Override
    public int run(String[] args) throws Exception {
        GenericOptionsParser gop = new GenericOptionsParser(args);
        setConf(gop.getConfiguration());
        CommandLine opts = processCmdline(gop.getRemainingArgs(), JOBNAME);
        getConf().setInt(FixedSplitInputFormat.FIXED_NUM_SPLITS, Integer.valueOf(opts.getOptionValue(OPT_SPLITS)));
        getConf().setInt(FIXED_SPLITS_NUMRECS, Integer.valueOf(opts.getOptionValue(OPT_NUMRECS, "100")));

        Job job = Job.getInstance(getConf(), JOBNAME);
        job.setJarByClass(EvenSplitJob.class);
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
        Option numRecsOption = new Option(OPT_NUMRECS, true, "number of records per mapper");
        numRecsOption.setRequired(false);
        numRecsOption.setArgName("record/mapper");
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
            for (int i = 0; i < numRecords; i++ ){
                VALUE_OUT.set(RandomStringUtils.randomAlphanumeric(RECORD_SIZE));
                context.write(key, VALUE_OUT);
            }
        }
    }

    /**
     * return the number of RECORD_SIZE records in a value given as 1M, 1G, 1K, etc.
     * @param sizeString a string value that represents number of bytes per mapper.  10G
     *                   would return (10 * 2^30)/RECORD_SIZE
     * @param recordSize size (in bytes) of standard output record
     * @return number of records to output
     */

    private int calcNumRecords(String sizeString, int recordSize){
        Pattern pat = Pattern.compile("(\\d+)(K|M|G)");
        return 0;
    }

}

