package org.oclc.hadoop.perf.mr.mapper;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.oclc.hadoop.perf.mr.WriteTableJob;

import java.io.IOException;

/**
 * This mapper writes each line of data to a row in HBase using the string linenumber of the data
 * as the rowkey.
 */
public class LineNumberRowMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Mutation> {

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
            byte[] rowKey = Long.toString(key.get()).getBytes();
            Put put = new Put(rowKey);
            put.addColumn(family, qualifier, Bytes.toBytes(data));
            context.write(new ImmutableBytesWritable(rowKey), put);
            context.getCounter(WriteTableJob.COUNTERS.LINES).increment(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
