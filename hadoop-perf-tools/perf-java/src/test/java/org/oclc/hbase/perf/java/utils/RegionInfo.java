package org.oclc.hbase.perf.java.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

/**
 * test access to region info.  uses the conf in test/resources/test-cluster-conf to connect to a cluster.
 */
public class RegionInfo {
    private static final Logger LOG = LoggerFactory.getLogger(RegionInfo.class);

    private Configuration conf = null;

    @Before
    public void setup() {
        conf = HBaseConfiguration.create();
        conf.addResource("test-cluster-conf/core-site.xml");
        conf.addResource("test-cluster-conf/hbase-site.xml");
    }

    @Test
    public void testConnect() {
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            LOG.info("connection to HBase acquired");
            Admin admin = conn.getAdmin();
            assertNotNull(admin);
            TableName[] tables = admin.listTableNames("hytest");
            assertThat("hytest not in list", tables.length, equalTo(1));
            Table hytest = conn.getTable(TableName.valueOf("hytest"));
            assertNotNull("hytest table get failed",hytest);

            hytest.close();
            admin.close();
            LOG.info("closed admin");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetRegions(){
        try (Connection conn = ConnectionFactory.createConnection(conf)){
            RegionLocator locator = conn.getRegionLocator(TableName.valueOf("Country"));
            List<HRegionLocation> locations = locator.getAllRegionLocations();
            assertTrue("list of regions is not empty", locations.size() > 0);
            for (HRegionLocation hl: locations){
                HRegionInfo ri = hl.getRegionInfo();
                LOG.info("server:{}, ri: {}, startKey: 0x{}, endKey: 0x{}",
                        hl.getHostname(),
                        ri.getRegionNameAsString(),
                        DatatypeConverter.printHexBinary(ri.getStartKey()),
                        DatatypeConverter.printHexBinary(ri.getEndKey()));
            }

            locator.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
