package org.oclc.hadoop.perf.mr.inputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DummyInputFormat extends InputFormat<Text, NullWritable> {

    public static final int NUM_SPLITS = 5;

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        List<InputSplit> splits = new ArrayList<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            splits.add(new DummyInputSplit());
        }
        return splits;
    }

    @Override
    public RecordReader<Text, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        DummyRecordReader reader = new DummyRecordReader();
        reader.initialize(inputSplit, taskAttemptContext);
        return reader;
    }
}
