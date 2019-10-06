package org.oclc.hadoop.perf.mr.inputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class DummyRecordReader extends RecordReader<Text, NullWritable> {
    public static final int MAX_RECS_OUT = 5;
    private int recsWritten = 0;
    private Text currKey;
    private NullWritable currVal=NullWritable.get();;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (recsWritten< MAX_RECS_OUT){
            currKey = new Text("dummy");
            recsWritten++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return currKey;
    }

    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return currVal;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0.0f;
    }

    @Override
    public void close() throws IOException {

    }
}
