package org.oclc.hadoop.perf.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.oclc.hadoop.perf.mr.inputformat.DummyInputFormat;

public class DummyWriterJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(new DummyWriterJob().run(args));
    }

    @Override
    public int run(String[] args) throws Exception {
        // don't need args for now
        GenericOptionsParser gop = new GenericOptionsParser(args);
//        Configuration conf = HBaseConfiguration.create(gop.getConfiguration());
        setConf(gop.getConfiguration());

        // setup job
        Job job = Job.getInstance(getConf(), "dummy");
        job.setJarByClass(DummyWriterJob.class);
        job.setInputFormatClass(DummyInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("xxx"));
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
