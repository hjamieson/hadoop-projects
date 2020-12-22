package org.oclc.hbase.analytics.jmx;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * class that has some understanding of the context of getting an jmx mbean from
 * a regionserver and preparing it for consumption as a json object to be indexed.
 */
public class JmxUtil {
    private static final Logger LOG = LoggerFactory.getLogger(JmxUtil.class);
    public static final String rsUrlTemplate = "http://%s:%s/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Server";
    private static final ObjectMapper om = new ObjectMapper();
    public static final String EVENT_TIME_LABEL = "eventTimeMs";
    private static TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
    };

    /**
     * reads the jmx bean from the host:port and returns the processed JSON.
     *
     * @param hostname the host
     * @param port     the rs port
     * @return json of the processed bean
     * @throws IOException
     */
    public static String getJmxAsJson(String hostname, int port) throws IOException {
        LOG.debug("initiate connection for {}:{}", hostname, port);
        return getJmxAsJson(new URL(String.format(rsUrlTemplate, hostname, port)));
    }

    /**
     * connects to the regionserver and returns the JMX bean as a map of values.
     * @param jsonUrl of the stream
     * @return a map of properties from the bean, with filtering an enrichmant applied.
     * @throws IOException
     */
    public static Map<String, Object> getJmxAsMap(URL jsonUrl) throws IOException {
        HashMap<String, Object> outerMap = om.readValue(jsonUrl, typeRef);
        List<Object> beans = (List<Object>) outerMap.get("beans");
        LinkedHashMap<String, Object> properties = (LinkedHashMap<String, Object>) beans.get(0);
        /*
        promote the hostname field to a first-class field (no tag.):
         */
        properties.put("hostname", properties.get("tag.Hostname"));
        Map<String, Object> enrichedMap = properties.entrySet().stream()
                .filter(kv -> !kv.getKey().endsWith("_percentile"))
                .filter(kv -> !kv.getKey().startsWith("tag."))
                .filter(kv -> !kv.getKey().startsWith("mob"))
                .filter(kv -> !kv.getKey().endsWith("_mean"))
                .filter(kv -> !kv.getKey().endsWith("_max"))
                .filter(kv -> !kv.getKey().endsWith("_min"))
                .filter(kv -> !kv.getKey().endsWith("_median"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        addNewProperties(enrichedMap);
        return enrichedMap;
    }

    /**
     * reads the jmx bean from the URL and returns the processed JSON.
     *
     * @param jsonUrl
     * @return the processed jmx bean in JSON format
     * @throws IOException
     */
    public static String getJmxAsJson(URL jsonUrl) throws IOException {
        return om.writeValueAsString(getJmxAsMap(jsonUrl));
    }

    private static void addNewProperties(Map<String, Object> map) {
        map.put(EVENT_TIME_LABEL, System.currentTimeMillis());
    }

}
