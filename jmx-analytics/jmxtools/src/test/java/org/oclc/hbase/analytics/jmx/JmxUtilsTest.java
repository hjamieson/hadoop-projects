package org.oclc.hbase.analytics.jmx;

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

import static org.junit.jupiter.api.Assertions.*;

/**
 * parse json using JsonUtils class.
 */
public class JmxUtilsTest {


    @Test
    void testBeanFromUrl() {
        try {
            URL url = new File("src/test/resources/jmx-sample-sub-server.json").toURI().toURL();
            System.out.println(url.toExternalForm());
            String testJson = JmxUtil.getJmxAsJson(url);
            System.out.println(testJson);
            assertTrue(testJson.length() > 0);
            assertTrue(testJson.startsWith("{"));
            assertTrue(testJson.endsWith("}"));
            assertTrue(testJson.contains("compactionQueueLength"), "map contains compaction key");
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    void testPercentageAreRemoved() {
        // get the raw map from the JSON:
        try {
            URL url = new File("src/test/resources/jmx-sample-sub-server.json").toURI().toURL();
            String testJson = JmxUtil.getJmxAsJson(url);
            assertTrue(!testJson.contains("_percentage"));
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testEnrichedFields() {
        try {
            URL url = new File("src/test/resources/jmx-sample-sub-server.json").toURI().toURL();
            Map<String, Object> map = JmxUtil.getJmxAsMap(url);
            assertTrue(map.containsKey("eventTimeMs"));
            assertTrue(map.containsKey("hostname"));
            assertEquals(map.get("hostname") ,"hddev1db014dxc1.dev.oclc.org");
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
            List<Object> list = (List<Object>) sob.get("beans");    // expect an ArrayList
            System.out.println(list.get(0).getClass().getName());   // expect a LinkedHashMap
            LinkedHashMap<String, Object> lmap = (LinkedHashMap<String, Object>) list.get(0);
            assert(lmap.containsKey("tag.Hostname"));
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
