package org.oclc.hbase.tools.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

/**
 * Provides access to RS data, such as the MBean/JSON metrics.
 * http://hddev1db014dxc1.dev.oclc.org:60030/jmx?qry=Hadoop:*
 * http://hddev1db014dxc1.dev.oclc.org:60030/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=IPC
 */
public class NetUtils {
    public static final Logger Log = LoggerFactory.getLogger(NetUtils.class);

    private NetUtils() {
    }


    /**
     * open the connection to the url and pull the json blob.  This returns the
     * JSON as a string.
     * @return JSON as string
     * @throws IOException
     */
    public static String getJmx(URL url) throws IOException {
        URLConnection con = url.openConnection();
        Log.debug("reading input stream from {}", url.toString());
        StringBuilder buffer = new StringBuilder();
        BufferedReader bis = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String line;
        while ((line = bis.readLine()) != null) {
            buffer.append(line);
        }
        return buffer.toString();
    }
}
