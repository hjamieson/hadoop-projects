package org.oclc.hbase.analytics.jmx;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * assuming that the jmx json is of '{beans:[{ field:value, field:value}]}',
 * reproduce the inner object and filter out the damages.
 */
public class ParseSubServer {

    @Test
    void testNestedObjects() {
        try {
            Map<String, String> map = new HashMap<>();
            JsonFactory jf = new JsonFactory();
            JsonParser jp = jf.createParser(new File("src/test/resources/jmx-sample-sub-server.json"));
            int numFields = 0;
            jp.nextToken();
            if (!jp.hasToken(JsonToken.START_OBJECT)) {
                throw new IllegalStateException("object is bad");
            }
            jp.nextToken();
            jp.nextToken();
            if (jp.currentToken() != JsonToken.START_ARRAY) {
                throw new IllegalStateException(String.format("expecitng array prolog; got [%s]!", jp.currentToken()));
            }
            jp.nextToken();
            if (jp.currentToken() != JsonToken.START_OBJECT) {
                throw new IllegalStateException("expecting START_OBJECT of array");
            }
            while (jp.currentToken() != JsonToken.END_OBJECT) {
                if (jp.currentToken() == JsonToken.FIELD_NAME) {
                    numFields++;
                    String field = jp.getCurrentName();
                    jp.nextToken();
                    String meta = jp.getCurrentToken() == JsonToken.VALUE_STRING ? "(s)" : "(n)";
                    String value = jp.getText();
                    map.put(field + meta, value);
                }
                jp.nextToken();
            }
            jp.close();
            assertTrue(numFields > 0);
            assertTrue(map.containsKey("compactionQueueLength(n)"));
            for (Map.Entry<String, String> e : map.entrySet()) {
                System.out.printf("k=%s, v=%s%n", e.getKey(), e.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

}
