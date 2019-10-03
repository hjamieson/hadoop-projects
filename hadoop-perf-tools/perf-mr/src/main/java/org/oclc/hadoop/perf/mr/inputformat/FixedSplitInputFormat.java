package org.oclc.hadoop.perf.mr.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class FixedSplitInputFormat extends InputFormat<Text, LongWritable> {

    public static final String FIXED_NUM_SPLITS = "fixed.num.splits";

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
        List<InputSplit> splits = new ArrayList<>();
        // lets test with 5
        Path outputDir = FileOutputFormat.getOutputPath(job);
        int numSplits = job.getConfiguration().getInt(FIXED_NUM_SPLITS, 1);
        for (int i = 0; i < numSplits; i++) {
            InputSplit split = new FileSplit(new Path(outputDir, String.format("%d", i)), 0, 1, (String[]) null);
            splits.add(split);
        }
        return splits;
    }

    @Override
    public RecordReader<Text, LongWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FixedSplitRecordReader(((FileSplit) inputSplit).getPath());
    }

    static class FixedSplitRecordReader extends RecordReader<Text, LongWritable> {
        Path name;
        Text key = null;
        LongWritable value = new LongWritable();

        public FixedSplitRecordReader(Path p) {
            name = p;
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (name != null) {
                key = new Text(name.getName());
                name = null;
                return true;
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public LongWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0.0f;
        }

        @Override
        public void close() throws IOException {
        }
    }
}
