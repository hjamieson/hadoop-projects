package org.oclc.hbase.region;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class TableRegions {

    private Connection conn;

    @BeforeEach
    void setup() throws IOException {
        conn = ConnectionFactory.createConnection();

    }

    @AfterEach
    void tearDown() throws IOException {
        conn.close();
    }

    @Test
    void listRegionsOfTableTest(){
        TableName tableName = TableName.valueOf("WorldcatXmlFragments");

        try {
            RegionLocator regionLocator = conn.getRegionLocator(tableName);
            List<HRegionLocation> allRegionLocations = regionLocator.getAllRegionLocations();
            System.out.println("Total regsions: " + allRegionLocations.size());
            allRegionLocations.stream()
                    .filter(rgn -> rgn.getHostname().startsWith("hddev1db032"))
                    .map(rgn -> rgn.getHostname())
                    .forEach(System.out::println);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
