package jmx.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * periodically queries a regionserver and dumps the jmx stats to stdout
 */
public class JmxPoll {
    private static final Logger LOG = LoggerFactory.getLogger(JmxPoll.class);
    private static String rsUrlTemplate;

    /**
     * takes a regionserver as input and polls jmx.
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        String index = "hbase-jmxserver-###";
        String ElkUrlTemplate = "http://###/#index#/_doc";
        String elkHost = "localhost:9200";
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
