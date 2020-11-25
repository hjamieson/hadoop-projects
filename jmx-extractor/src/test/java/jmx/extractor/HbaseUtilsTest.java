package jmx.extractor;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * abstracts away the HBase-ness of the domain.
 */
public class HbaseUtilsTest {
    @Test
    void testGetRsNames() {
        List<String> regionServersList = HbaseUtils.getRegionServersList();
        assertTrue(regionServersList.size()!= 0);
        regionServersList.forEach(r -> System.out.println(r));
    }
}
