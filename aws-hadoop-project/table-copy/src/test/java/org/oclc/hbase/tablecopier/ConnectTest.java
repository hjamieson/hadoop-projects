package org.oclc.hbase.tablecopier;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

public class ConnectTest {
    @Test
    public void testConnection() {
        Configuration hcf = HBaseConfiguration.create();
        hcf.set("hbase.zookeeper.quorum", "hdchbadb001pxh1.csb.oclc.org:2181");
        try (
                Connection con = ConnectionFactory.createConnection(hcf);
                Admin admin = con.getAdmin();
        ) {
            assertNotNull(admin);
            for (TableName t: admin.listTableNames()){
                System.out.println(t.getNameWithNamespaceInclAsString());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
