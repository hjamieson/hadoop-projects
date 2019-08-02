package org.oclc.hbase.perf.java.utils;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Prints information about all the regions in a given table.  The output is a
 * single JSON object per region, each on its own line.  This allows you to
 * use the output with other tools like JQ or spark.
 * <p>
 * This class will look for cluster connection details in the classpath.
 * <p>
 * eg: hbase org.oclc.hbase.perf.java.utils.PrintRegionInfo Country
 * hbase org.oclc.hbase.perf.java.utils.PrintRegionInfo cmops:CmopsTable
 */
public class PrintRegionInfo {


    public static void main(String[] args) {
        /*
         * we take a single argument: table
         */
        if (args.length != 1) {
            System.out.printf("Usages: %s <table-name>\n\n", PrintRegionInfo.class.getSimpleName());
            System.exit(1);
        }

        try (Connection conn = ConnectionFactory.createConnection()) {
            if (!conn.getAdmin().tableExists(TableName.valueOf(args[0]))) {
                throw new IOException("table [" + args[0] + "] not found");
            }

            RegionLocator rl = conn.getRegionLocator(TableName.valueOf(args[0]));
            Stream<HRegionLocation> regions = rl.getAllRegionLocations().stream();
            rl.close();


            regions.map(PrintRegionInfo::regionInfoCollector)
                    .forEach(toStdOut);

        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

    static RegionObject regionInfoCollector(HRegionLocation location) {
        RegionObject ro = new RegionObject();
        HRegionInfo ri = location.getRegionInfo();
        ro.setRegionNameAsString(ri.getRegionNameAsString());
        return ro;
    }

    static Consumer<RegionObject> toStdOut = (RegionObject ro) -> {
        System.out.println(ro.toString());
    };


    static class RegionObject {
        String regionNameAsString;

        @Override
        public String toString() {
            return String.format("{%s}", regionNameAsString);
        }

        public String getRegionNameAsString() {
            return regionNameAsString;
        }

        public void setRegionNameAsString(String regionNameAsString) {
            this.regionNameAsString = regionNameAsString;
        }
    }
}
