package org.oclc.hbase.analytics.jmx;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.fail;

public class GetRsJmxSample {
    URL url = new URL(HbaseUtils.getSubServerUrl("hddev1db014dxc1.dev.oclc.org"));

    public GetRsJmxSample() throws MalformedURLException {
    }
    @Test
    void downloadRegionServerJmxSample() {
        try {
            String jmx = NetUtils.getJmx(url);
            assert(jmx.length() > 0);
            Path output = Paths.get("build/tmp/rs-jmx-sample.json");
            Files.write(output, jmx.getBytes());
            assert(Files.exists(output));

        } catch (IOException e) {
            fail(e);
        }
    }

}