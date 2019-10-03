package org.oclc.hadoop.perf.mr;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class UtilsTest {
    @Test
    public void testByteString(){
        assertThat("2k should == 2048", Utils.byteStringToLong("2k"),is(2048l));
        assertThat("2K should == 2048", Utils.byteStringToLong("2K"),is(2048l));
        assertThat("2M should == 2097152", Utils.byteStringToLong("2M"),is(((long) (2 * Math.pow(2, 20)))));
        assertThat("m or M case works", Utils.byteStringToLong("2m"),is(2097152L));
        assertThat("4G should == ", Utils.byteStringToLong("4G"),is(((long) (4 * Math.pow(2, 30)))));
        assertThat("g or G case works", Utils.byteStringToLong("4G"),is(((long) (4 * Math.pow(2, 30)))));

    }
}
