package org.oclc.hbase.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.util.List;

/**
 * Given an HBase table, produce metrics for the table that can be used for analytics.
 * <p>
 * You should be able to run this using the hadoop command:
 * $hadoop jar target.jar [table-name]
 */
public class TableAnalyzer implements AutoCloseable {

    private Connection conn;
    private Admin admin;
    private FileSystem fs;

    public static void main(String[] args) {

        try (TableAnalyzer tableAnalyzer = new TableAnalyzer(args[0], HBaseConfiguration.create());) {
            tableAnalyzer.getRegionList();
            System.exit(0);
        } catch (Exception e) {
            System.err.println("abend: " + e.getMessage());
            System.exit(1);
        }
    }

    private TableName tableName;
    private Configuration conf;


    public TableAnalyzer(String table, Configuration conf) throws IOException {
        this.tableName = TableName.valueOf(table);
        this.conf = conf;
        init();
    }

    private void init() throws IOException {
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
        // make sure our table exists:
        if (!admin.tableExists(tableName)) {
            throw new IllegalArgumentException("table " + tableName.getNameAsString() + " does not exist");
        }
        fs = FileSystem.get(conf);
    }

    @Override
    public void close() {
        try {
            admin.close();
        } catch (Throwable ignore) {
        }
        try {
            conn.close();
        } catch (Throwable ignore) {
        }
    }

    public void getRegionList() throws IOException {
        List<HRegionInfo> tableRegions = admin.getTableRegions(tableName);
        for (HRegionInfo hri : tableRegions) {
            long regionSize = getRegionSize(hri);
            System.out.println(
                    String.format("%s:%s,%s,%s,%d.%s.,%s,%s,%d",
                            hri.getTable().getNamespaceAsString(),
                            hri.getTable().getQualifierAsString(),
                            DatatypeConverter.printHexBinary(hri.getStartKey()),
                            DatatypeConverter.printHexBinary(hri.getEndKey()),
                            hri.getRegionId(),
                            hri.getEncodedName(),
                            hri.isSplit() ? "split" : "nosplit",
                            hri.isSplitParent() ? "split-parent": "parent",
                            regionSize));
        }

    }

    private long getRegionSize(HRegionInfo hri){
        // grab the physical region.  It should resolve to /hbase/data/{ns}/{table}/{regionname}
        String hdfsRegion = String.format("%s/data/%s/%s/%s",
                conf.get("hbase.rootdir","/hbase"),
                hri.getTable().getNamespaceAsString(),
                hri.getTable().getQualifierAsString(),
                hri.getEncodedName());
        System.err.println(hdfsRegion);
        Path region = new Path(hdfsRegion);
        try {
            return fs.getFileStatus(region).getLen();
        } catch (IOException e) {
            return -1;
        }
    }
}
