package org.oclc.hbase.region.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Arrays;

public class ConnectionTest {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("/hbase-site.xml");

        try {
            Connection con = ConnectionFactory.createConnection(conf);
            Admin admin = con.getAdmin();
            Arrays.stream(admin.listTableNames()).forEach(System.out::println);
            admin.close();
            con.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
