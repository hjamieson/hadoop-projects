package org.oclc.hbase.tools.extractor.job;

import org.oclc.hbase.tools.extractor.ElkUtils;
import org.oclc.hbase.tools.extractor.HbaseUtils;
import org.oclc.hbase.tools.extractor.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * periodically queries all jmx beans and dumps the jmx stats to an
 * elasticsearch cluster as an hbase-jmxserver-YYYY-MM-DD index.
 */
public class Jmx2Elk {
    private static final Logger LOG = LoggerFactory.getLogger(Jmx2Elk.class);
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
        String index = "hbase-jmxserver-###";
        String ElkUrlTemplate = "http://###/#index#/_doc";
        String elkHost = System.getProperty("ES_ADDRESS","localhost:9200");
        rsUrlTemplate = "http://###:60030/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Server";


        while (true) {
            List<String> regionServers = HbaseUtils.getRegionServersList();
            if (regionServers.size() == 0) {
                LOG.info("no regionServers are found; exiting");
                System.exit(0);
            }
            regionServers.forEach(r -> LOG.debug("regionserver: {}", r));

            for (String rs : regionServers) {
                URI elkUri = new URI(ElkUrlTemplate.replace("###", elkHost).replace("#index#", index.replace("###", LocalDate.now().toString())));
                URI jmxUri = new URI(rsUrlTemplate.replace("###", rs));
                Map<String, Object> map = JsonUtils.jsonToMap(jmxUri.toURL());
                ElkUtils.post(elkUri, JsonUtils.enrich(map));
            }
            Thread.sleep(30000);
        }
    }
}
