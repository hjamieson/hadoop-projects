package org.oclc.hbase.analytics.jmx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Dummy {
    private static final Logger LOG = LoggerFactory.getLogger(Dummy.class);
    public static void main(String[] args) {
        List<String> regionServersList = HbaseUtils.getRegionServersList();
        regionServersList.stream().forEach(rs -> System.out.println(rs));
    }
}
