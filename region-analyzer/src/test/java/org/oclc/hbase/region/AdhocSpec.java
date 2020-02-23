package org.oclc.hbase.region;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;

public class AdhocSpec {
    List<String> words = Arrays.asList("now is the time for all good men to come to the aid of their party".split(" "));

    @Test
    void groupByTest() {
        Map<String, List<String>> grouped = words.stream()
                .collect(groupingBy(s -> s));
        System.out.println(grouped);
    }
}
