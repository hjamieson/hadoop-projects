package org.oclc.hadoop.perf.mr;

import org.apache.commons.cli.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WriteTableJob extends Configured implements Tool {
    public static final Logger LOG = LoggerFactory.getLogger(WriteTableJob.class);
    public static final String OPT_TABLE = "t";
    public static final String OPT_COLUMN = "c";
    public static final String OPT_FILE = "f";
    public final String JOBNAME = "xxx";

    public enum COUNTERS {LINES}

    public static void main(String[] args) throws Exception {
        System.exit(new WriteTableJob().run(args));
    }

    @Override
    public int run(String[] args) throws Exception {
        GenericOptionsParser gop = new GenericOptionsParser(args);
        setConf(HBaseConfiguration.create(gop.getConfiguration()));
        CommandLine opts = processCmdline(gop.getRemainingArgs(), JOBNAME);
        TableName table = TableName.valueOf(opts.getOptionValue(OPT_TABLE));

        Job job = Job.getInstance(getConf(), JOBNAME);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table.getNameWithNamespaceInclAsString());
        job.getConfiguration().set("conf.column", opts.getOptionValue(OPT_COLUMN));
        job.setJarByClass(WriteTableJob.class);
        job.setMapperClass(WriteFileMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(opts.getOptionValue(OPT_FILE)));
        job.setOutputFormatClass(TableOutputFormat.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setNumReduceTasks(0);
        TableMapReduceUtil.addDependencyJars(job);

        int rc = job.waitForCompletion(true)? 0 : 1 ;

        return rc;
    }

    private static CommandLine processCmdline(String[] args, String jobname) {
        Options options = new Options();
        Option tableOpt = new Option(OPT_TABLE, true, "table name");
        tableOpt.setRequired(true);
        options.addOption(tableOpt);
        Option colOpt = new Option(OPT_COLUMN, true, "column to store data");
        colOpt.setRequired(true);
        colOpt.setArgName("family:qualifier");
        options.addOption(colOpt);
        Option fileOpt = new Option(OPT_FILE, true, "source file data");
        fileOpt.setRequired(true);
        options.addOption(fileOpt);
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

    public static class WriteFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Mutation> {

        private byte[] family;
        private byte[] qualifier;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String column = context.getConfiguration().get("conf.column");
            byte[][] colKey = KeyValue.parseColumn(Bytes.toBytes(column));
            family = colKey[0];
            if (colKey.length > 1) {
                qualifier = colKey[1];
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String data = value.toString();
                byte[] rowKey = DigestUtils.md5(data);
                Put put = new Put(rowKey);
                put.addColumn(family, qualifier, Bytes.toBytes(data));
                context.write(new ImmutableBytesWritable(rowKey), put);
                context.getCounter(COUNTERS.LINES).increment(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
