package org.oclc.hadoop.perf.mr.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class FakeInputFormat extends InputFormat<Text, Text> {

    public static final String NUM_SPLITS = "dummy.num.splits";

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
        List<InputSplit> splits = new ArrayList<>();
//        Path outputDir = FileOutputFormat.getOutputPath(job);
        Path outputDir = new Path("/tmp","dummy");
        int numSplits = job.getConfiguration().getInt(NUM_SPLITS, 1);
        for (int i = 0; i < numSplits; i++) {
            InputSplit split = new FileSplit(new Path(outputDir, String.format("%d", i)), 0, 1, (String[]) null);
            splits.add(split);
        }
        return splits;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FakeRecordReader(((FileSplit) inputSplit).getPath());
    }

}
