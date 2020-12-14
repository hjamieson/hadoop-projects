package org.oclc.hbase.analytics.jmx;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ElkUtilsTest {
    @Test
    void testBulkFormat(){
        Map<String, Object> map = new HashMap<>();
        map.put("name","hugh");
        map.put("cost", 11.99);
        map.put("found", true);
        String bulk = ElkUtils.toBulkFormat(map, "test123");
        /* expected:
        {"index":{"_index":"test123"}} (lf)
        {"name":"hugh","cost":11.11, "found": true}(lf)
         */
        System.out.println(bulk);
        assertEquals(2, bulk.chars().filter(c -> c == '\n').count());
    }
}
