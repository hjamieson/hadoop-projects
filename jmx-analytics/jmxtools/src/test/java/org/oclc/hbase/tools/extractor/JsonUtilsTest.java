package org.oclc.hbase.tools.extractor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * parse json using JsonUtils class.
 */
public class JsonUtilsTest {

    @Test
    void testBeanToMap() {
        try {
            JsonFactory jf = new JsonFactory();
            JsonParser jp = jf.createParser(new File("src/test/resources/jmx-sample-sub-server.json"));
            while (jp.nextToken() != JsonToken.START_ARRAY) {
                jp.nextToken();
            }
            jp.nextToken();
            Map<String, Object> map = JsonUtils.jsonToMap(jp);
            jp.close();
            assertTrue(map.size() > 0, "map is not empty");
            assertTrue(map.containsKey("compactionQueueLength"), "map contains unique key");
            for (Map.Entry<String, Object> e : map.entrySet()) {
                System.out.printf("k=%s, v=%s%n", e.getKey(), e.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testBeanFromUrl() {
        try {
            URL url = new File("src/test/resources/jmx-sample-sub-server.json").toURI().toURL();
            System.out.println(url.toExternalForm());
            Map<String, Object> map = JsonUtils.jsonToMap(url);
            assertFalse(map.isEmpty(), "map should contain elements");
            assertTrue(map.containsKey("compactionQueueLength"), "map contains compaction key");
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    void testWriteMapOfPrimitives() {
        try {
            Map<String, Object> map = new HashMap<>();
            map.put("name", "huey");
            map.put("intValue", 25);
            map.put("longValue", Long.valueOf(1024l));
            map.put("booleanValue", true);
            map.put("doubleValue", 123.45d);

            ObjectMapper om = new ObjectMapper();
            String json = om.writeValueAsString(map);
            System.out.println(json);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testPercentageAreRemoved() {
        // get the raw map from the JSON:
        try {
            URL url = new File("src/test/resources/jmx-sample-sub-server.json").toURI().toURL();
            Map<String, Object> map = JsonUtils.jsonToMap(url);
            // remove everything that does not end with _percentage
            Map<String, Object> cleanMap = map.entrySet().stream().filter(e -> e.getKey().endsWith("_percentage")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            assertTrue(cleanMap.isEmpty(), "percentage removed");
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testEnrichedFields() {
        try {
            URL url = new File("src/test/resources/jmx-sample-sub-server.json").toURI().toURL();
            Map<String, Object> map = JsonUtils.jsonToEnrichedMap(url);
            // remove everything that does not end with _percentage
            assertTrue(map.containsKey("shortHostname"));
            assertEquals("hddev1db014dxc1", map.get("shortHostname"));
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testUseMapperToReadBean(){
        ObjectMapper om = new ObjectMapper();
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};
        try {
            File file = new File("src/test/resources/jmx-sample-sub-server.json");
            HashMap<String, Object> sob = om.readValue(file, typeRef);
            assert(sob.containsKey("beans"));
            System.out.println(sob);
//            System.out.println(sob.get("beans").getClass().getName());
            List<Object> list = (List<Object>) sob.get("beans");    // expect an ArrayList
            System.out.println(list.get(0).getClass().getName());   // expect a LinkedHashMap
            LinkedHashMap<String, Object> lmap = (LinkedHashMap<String, Object>) list.get(0);
            lmap.forEach((k, v) -> System.out.printf("key: %s, v: %s%n", k, v.getClass().getName()));
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
