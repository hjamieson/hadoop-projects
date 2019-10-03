package org.oclc.hadoop.perf.mr;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.oclc.hadoop.perf.mr.mapper.LineNumberRowMapper;
import org.oclc.hadoop.perf.mr.mapper.MD5RowMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WriteTableUsingLineNumberJob extends Configured implements Tool {
    public static final Logger LOG = LoggerFactory.getLogger(WriteTableUsingLineNumberJob.class);
    public static final String OPT_TABLE = "t";
    public static final String OPT_COLUMN = "c";
    public static final String OPT_FILE = "f";
    public final String JOBNAME = "xxx";

    public enum COUNTERS {LINES}

    public static void main(String[] args) throws Exception {
        System.exit(new WriteTableUsingLineNumberJob().run(args));
    }

    @Override
    public int run(String[] args) throws Exception {
        GenericOptionsParser gop = new GenericOptionsParser(args);
        setConf(HBaseConfiguration.create(gop.getConfiguration()));
        CommandLine opts = processCmdline(gop.getRemainingArgs(), JOBNAME);
        TableName table = TableName.valueOf(opts.getOptionValue(OPT_TABLE));

        checkTableExists(table);

        Job job = Job.getInstance(getConf(), JOBNAME);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table.getNameWithNamespaceInclAsString());
        job.getConfiguration().set("conf.column", opts.getOptionValue(OPT_COLUMN));
        job.setJarByClass(WriteTableUsingLineNumberJob.class);
        job.setMapperClass(LineNumberRowMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(opts.getOptionValue(OPT_FILE)));
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

}
