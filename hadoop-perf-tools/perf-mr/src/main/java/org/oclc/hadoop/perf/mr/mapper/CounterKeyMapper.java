package org.oclc.hadoop.perf.mr.mapper;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.oclc.hadoop.perf.mr.Utils;

import java.io.IOException;

/**
 * This mapper uses a counter in HBase to provide the sequential ascending
 * keys for the rows.  This mapper needs to know the name of the sequence
 * table, the row, and the col to use.  These facts will be passed to the
 * mapper via conf parameters.
 */
public class CounterKeyMapper extends Mapper<Text, Text, ImmutableBytesWritable, Mutation> {
    public static final String COUNTERKEYMAPPER_NUMROWS = "counterkeymapper.numrows";
    ImmutableBytesWritable DUMMY = new ImmutableBytesWritable();
    byte[] CF = "d".getBytes();
    byte[] COL = "c1".getBytes();
    byte[] COUNTER_ROW = "sequence".getBytes();
    byte[] COUNTER_FAM = "counter".getBytes();
    byte[] COUNTER_COL = "next".getBytes();
    private Table sequenceTable;
    private Connection con;
    private int maxRows;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        CF = "d".getBytes();
        COL = "c1".getBytes();
        con = ConnectionFactory.createConnection(context.getConfiguration());
        sequenceTable = con.getTable(TableName.valueOf(context.getConfiguration().get("sequence.table.name")));
        maxRows = context.getConfiguration().getInt(COUNTERKEYMAPPER_NUMROWS, 1);
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        /*
        we use the counter to get the next key; we will ask for a block of values
        so we are not incrementing for each put
         */
        long lastKey = sequenceTable.incrementColumnValue(COUNTER_ROW, COUNTER_FAM, COUNTER_COL, maxRows);
        for (int i = 0; i < maxRows; i++) {
            Put put = new Put(Long.toString(lastKey-maxRows + i).getBytes());
            put.addColumn(CF, COL, Utils.randomContent(Utils.CONTENT.RANDOMTEXT));
            context.write(DUMMY, put);
            if (i % 100 == 0) {
                context.setStatus(String.format("%d written (%s)", i,
                        StringUtils.formatPercent((double) i / (double) maxRows, 1)));
                context.progress();
            }
        }
        context.setStatus(String.format("%d written (%s)", maxRows,
                StringUtils.formatPercent(1.0d, 1)));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        sequenceTable.close();
        con.close();
    }
}
