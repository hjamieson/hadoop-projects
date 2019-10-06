package org.oclc.hadoop.perf.mr;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class UtilsTest {
    @Test
    public void testByteString() {
        assertThat("2k should == 2048", Utils.byteStringToLong("2k"), is(2048l));
        assertThat("2K should == 2048", Utils.byteStringToLong("2K"), is(2048l));
        assertThat("2M should == 2097152", Utils.byteStringToLong("2M"), is(((long) (2 * Math.pow(2, 20)))));
        assertThat("m or M case works", Utils.byteStringToLong("2m"), is(2097152L));
        assertThat("4G should == ", Utils.byteStringToLong("4G"), is(((long) (4 * Math.pow(2, 30)))));
        assertThat("g or G case works", Utils.byteStringToLong("4G"), is(((long) (4 * Math.pow(2, 30)))));

    }

    @Test
    public void testTextContent() {
        byte[] c = Utils.randomContent(Utils.CONTENT.TEXT);
        assertThat("text is not empty", c.length, not(0));
        byte[] json = Utils.randomContent(Utils.CONTENT.JSON);
        assertThat("json is not empty", json.length, not(0));
        ObjectMapper om = new ObjectMapper();
        try {
            om.readValue(json, new TypeReference<Map<String, Object>>(){});
        } catch (IOException e) {
            fail(e.getMessage());
        }
        byte[] xml = Utils.randomContent(Utils.CONTENT.XML);
        assertThat("xml is not empty", xml.length, not(0));

    }
}
