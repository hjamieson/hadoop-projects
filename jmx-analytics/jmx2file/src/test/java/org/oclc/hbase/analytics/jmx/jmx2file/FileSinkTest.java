package org.oclc.hbase.analytics.jmx.jmx2file;

import org.junit.jupiter.api.Test;
import org.oclc.hbase.analytics.jmx.collector.JmxSink;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class FileSinkTest {
    String testFile = "build/tmp/test.txt";

    @Test
    void testInit() {
        JmxSink sink = new FileSink(new File(testFile));
        try {
            sink.init(new Properties());
        } catch (IOException e) {
            fail(e);
        }
        List<String> originals = Arrays.asList(new String[]{"one", "two", "three"});
        originals.forEach(s -> {
            try {
                sink.write(s);
            } catch (IOException e) {
                fail(e.getMessage());
            }
        });
        sink.close();

        // read the file and verify the contents:
        try {
            List<String> strings = Files.readAllLines(FileSystems.getDefault().getPath(testFile));
            assertTrue(strings.containsAll(originals));
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    @Test
    void testClose() {
        JmxSink sink = new FileSink(new File(testFile));
        try {
            sink.init(new Properties());
            for (int i = 0; i < 100; i++){
                sink.write(String.format("this is line %d", i));
            }
            sink.close();

            // make sure there are 100 entries:
            List<String> strings = Files.readAllLines(FileSystems.getDefault().getPath(testFile));
            assert(strings.size()== 100);
        } catch (Throwable t){
            fail(t);
        }

    }
}
