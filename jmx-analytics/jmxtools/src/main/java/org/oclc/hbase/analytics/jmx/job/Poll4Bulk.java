package org.oclc.hbase.analytics.jmx.job;

import org.oclc.hbase.analytics.jmx.ElkUtils;
import org.oclc.hbase.analytics.jmx.HbaseUtils;
import org.oclc.hbase.analytics.jmx.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * Job that polls the RSs and writes the data to a file in elk bulk load format.
 */
public class Poll4Bulk {
    private static final Logger LOG = LoggerFactory.getLogger(Poll4Bulk.class);
    private static final String INDEXTEMPLATE = "hbase-jmxserver-###";

    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        // use args[0] as a filename; stdout if missing
        Writer writer ;
        if (args.length > 0){
            writer = new FileWriter(args[0]);
        } else {
            writer = new OutputStreamWriter(System.out);
        }

        while (true) {
            List<String> regionServers = HbaseUtils.getRegionServersList();
            if (regionServers.size() == 0) {
                LOG.info("no regionServers found; exiting");
                System.exit(0);
            }
            regionServers.forEach(r -> LOG.debug("regionserver: {}", r));

            for (String rs : regionServers) {
                URI jmxUri = new URI(HbaseUtils.getSubServerUrl(rs));
                Map<String, Object> map = JsonUtils.jsonToEnrichedMap(jmxUri.toURL());
                writer.write(ElkUtils.toBulkFormat(map, INDEXTEMPLATE.replace("###",LocalDate.now().toString())));
            }
            writer.flush();
            Thread.sleep(30000);
        }
    }
}
