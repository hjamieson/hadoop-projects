package org.oclc.hbase.perf.java.utils;

import org.apache.hadoop.hbase.HRegionLocation;

@FunctionalInterface
interface RegionInfoToString {
    String map(HRegionLocation ri);
}
