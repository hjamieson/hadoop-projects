package org.oclc.hbase.nowaltool;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseTool {
    static String putToTable(String table, String rowKey, String col, String value) throws IOException {
        TableName tableName = TableName.valueOf(table);
        byte[] key = Bytes.toBytes(rowKey);
        String[] fmcl = col.split(":");
        byte[] cf = Bytes.toBytes(fmcl[0]);
        byte[] cq = Bytes.toBytes(fmcl[1]);
        byte[] val = Bytes.toBytes(value);
        byte[] latestValue = putToTable(tableName, key, cf, cq, val);
        return Bytes.toString(latestValue);
    }

    static byte[] putToTable(TableName tableName, byte[] rowKey, byte[] cf, byte[] cq, byte[] value) throws IOException {
        try (Connection con = ConnectionFactory.createConnection(); Table table = con.getTable(tableName)) {
            Put put = new Put(rowKey);
            put.setDurability(Durability.SKIP_WAL);
            put.addColumn(cf, cq, value);
            table.put(put);

            // now get the value
            Get get = new Get(rowKey);
            get.addColumn(cf, cq);
            Result result = table.get(get);
            return result.getValue(cf, cq);

        }
    }
}
