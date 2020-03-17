package org.oclc.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class SimpleScan {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hddev1db001dxc1.dev.oclc.org");
        try (Connection con = ConnectionFactory.createConnection(conf)) {

            testScanResults(con, TableName.valueOf("Worldcat"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void testScanResults(Connection con, TableName tableName) {
        Scan scan = new Scan(Bytes.toBytes("1"), Bytes.toBytes("1"));
        scan.setMaxVersions(1);
        scan.addFamily(Bytes.toBytes("data"));
        scan.setFilter(new FirstKeyOnlyFilter());
        try (Table table = con.getTable(tableName); ResultScanner scanner = table.getScanner(scan);) {
            scanner.forEach(r -> System.out.println("size= " +r.size()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
