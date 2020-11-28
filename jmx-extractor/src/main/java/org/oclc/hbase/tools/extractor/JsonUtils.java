package org.oclc.hbase.tools.extractor;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import org.oclc.hbase.tools.extractor.model.MetaField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class);
    private static final ObjectMapper om = new ObjectMapper();

    /**
     * takes a JsonParser (positioned on a START_OBJECT token) and creates a map
     * of each field:value.  Note; we do not close the parser!
     *
     * @param jp
     */
    static Map<String, Object> jsonToMap(JsonParser jp) throws IOException {
        Map<String, Object> map = new HashMap<>();
        while (jp.currentToken() != JsonToken.END_OBJECT) {
            if (jp.currentToken() == JsonToken.FIELD_NAME) {
                String fieldName = jp.getCurrentName();
                // remove any tag. keys here
                if (fieldName.startsWith("tag.")){
                    fieldName = fieldName.replace("tag.","");
                }
                jp.nextToken();
                switch (jp.currentToken()) {
                    case VALUE_STRING:
                        map.put(fieldName, jp.getText());
                        break;
                    case VALUE_NUMBER_INT:
                        map.put(fieldName, jp.getLongValue());
                        break;
                    case VALUE_FALSE:
                    case VALUE_TRUE:
                        map.put(fieldName, jp.getBooleanValue());
                        break;
                    case VALUE_NUMBER_FLOAT:
                        map.put(fieldName, jp.getDoubleValue());
                        break;
                    default:
                        throw new IllegalArgumentException("unexpected json object token: " + jp.currentToken());
                }
            }
            jp.nextToken();
        }
        return map;
    }

    /**
     * pulls the jmx bean from the rs, then parses the json to extract the first object
     * inside the bean[] we get from the jmx endpoint.  The json looks like this:
     * { "beans":[{key:val, key:val}}
     * This implies that we have to walk the json until we are positioned at the first
     * object in the "beans" array.
     *
     * @param rsUrl regionserver jmx uri
     * @return
     * @throws IOException
     */
    public static Map<String, Object> jsonToMap(URL rsUrl) throws IOException {
        LOG.debug("pulling jmx bean from {}", rsUrl.toExternalForm());
        Stopwatch sw = new Stopwatch().start();
        // removed to be compatible with jackson in hbase stack
//        JsonFactory jf = JsonFactory.builder()
//                .configure(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS, true)
//                .build();
        JsonFactory jf = new JsonFactory();
        JsonParser jp = jf.createParser(NetUtils.getJmx(rsUrl));
        seekToFirstObjectInArray(jp);   // positions us on the first field
        Map<String, Object> map = jsonToMap(jp);
        jp.close();
        sw.stop();
        LOG.debug("polling jmx data from {}, sw={}", rsUrl, sw.toString());
        return map;
    }

    /**
     * moves the parser to the first object in the array.  We throw exceptions
     * if our assumptions about the bean are not met.
     *
     * @param jp
     */
    private static void seekToFirstObjectInArray(JsonParser jp) throws IOException {
        if (jp.nextToken() != JsonToken.START_OBJECT) {
            throw new IllegalStateException("START_OBJECT expected: was " + jp.currentToken());
        }
        if (jp.nextToken() != JsonToken.FIELD_NAME) {
            throw new IllegalStateException("START_ARRAY expected: was " + jp.currentToken());
        }
        if (jp.nextToken() != JsonToken.START_ARRAY) {
            throw new IllegalStateException("START_ARRAY expected: was " + jp.currentToken());
        }
        if (jp.nextToken() != JsonToken.START_OBJECT) {
            throw new IllegalStateException("START_OBJECT expected: was " + jp.currentToken());
        }
    }

    /**
     * enrich the bean with extra data, such as a timestamp field.
     *
     * @param map
     * @return
     */
    public static Map<String, Object> enrich(Map<String, Object> map) {
        map.put("@timestamp", System.currentTimeMillis());
        map.put("shortHostname", map.get("Hostname").toString().split("\\.")[0]);

        // remove the percentage fields until we decide we want em!
        Stream<Map.Entry<String, Object>> percentage = map.entrySet().stream().filter(e -> !e.getKey().endsWith("_percentile"));
        return percentage.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * returns the enriched version of the map from the rs.  This is a convenience method.
     */
    public static Map<String, Object> jsonToEnrichedMap(URL rsUrl) throws IOException {
        return enrich(jsonToMap(rsUrl));
    }

    /**
     * takes a metafield map and writes it back as a JSON object.
     *
     * @param map
     * @return a JSON string
     */
    public static String mapToJson(Map<String, MetaField> map) throws JsonProcessingException {
        return om.writeValueAsString(map);
    }
}
