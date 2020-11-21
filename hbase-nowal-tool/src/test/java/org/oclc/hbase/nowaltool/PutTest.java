package org.oclc.hbase.nowaltool;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class PutTest {
    @Test
    void putToDevTest() {

        try {
            String testValue = LocalDateTime.now().toString();
            String latest = HBaseTool.putToTable("hytest", "huey", "f1:c1", testValue);
            assertEquals(testValue, latest);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }
}
