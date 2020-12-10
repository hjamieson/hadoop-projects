package org.oclc.hbase.analytics.jmx;

import org.oclc.hbase.tools.extractor.HbaseUtils;
import org.oclc.hbase.tools.extractor.JmxUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

/**
 * periodically queries all jmx beans and dumps the jmx stats to stdout
 */
public class Jmx2Stdout {
    private static final Logger LOG = LoggerFactory.getLogger(Jmx2Stdout.class);
    private static String rsUrlTemplate;

    /**
     * takes a regionserver as input and polls jmx.  we look for the following
     * variables in the environment:
     * ES_ADDRESS : the host:ip of the elasticsearch server (default: localhost:9200)
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        int port = 60030;
        while (true) {
            List<String> regionServers = HbaseUtils.getRegionServersList();
            if (regionServers.size() == 0) {
                LOG.info("no regionServers are found; exiting");
                System.exit(0);
            }
            regionServers.forEach(r -> LOG.debug("regionserver: {}", r));

            for (String rs : regionServers) {
                System.out.println(JmxUtil.getJmxAsJson(rs, port));
            }
            Thread.sleep(30000);
        }
    }
}
