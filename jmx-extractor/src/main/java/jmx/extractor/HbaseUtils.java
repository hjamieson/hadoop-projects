package jmx.extractor;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * abstracts away the HBase-ness of the domain.
 */
public class HbaseUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HbaseUtils.class);

    static final List<String> testList = Arrays.asList("hddev1db008dxc1.dev.oclc.org", "hddev1db009dxc1.dev.oclc.org", "hddev1db010dxc1.dev.oclc.org");

    /**
     * returns the list of regionservers for the current environment MINUS any dead servers.
     * @return
     */
    public static List<String> getRegionServersList(){
        List<String> results = new ArrayList<>();
        try (Connection con = ConnectionFactory.createConnection(); Admin admin = con.getAdmin();){
            ClusterStatus clusterStatus = admin.getClusterStatus();
            Collection<ServerName> servers = clusterStatus.getServers();
            servers.forEach(svr -> results.add(svr.getHostname()));
            Collection<ServerName> deadServerNames = clusterStatus.getDeadServerNames();
            deadServerNames.forEach(ds -> results.remove(ds.getHostname()));
        } catch (IOException e) {
            LOG.error("failed to connect to hbase", e);
        }
        return results;
    }
}
