package org.oclc.hadoop.perf.mr;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Any shared utility methods can be found here.
 */
public class Utils {
    public static String rgx = "(\\d+)(K|M|G)";
    private static Pattern pat = Pattern.compile(rgx);
    enum MAG {K, M, G}

    ;

    /**
     * takes a string argument in the form of 10G, 5K, 45M and returns a long value
     * of the number of bytes it represents.  For example, 1K = 1024, 1M=2^20, 1G = 2^30.
     * This is used to parse commandline arguments.
     *
     * @param byteString
     * @return
     */
    public static long byteStringToLong(String byteString) {
        Matcher matcher = pat.matcher(byteString.toUpperCase());
        if (!matcher.matches()) {
            throw new IllegalArgumentException(String.format("[%s] invalid size request", byteString));
        }
        long ord = Long.valueOf(matcher.group(1));
        switch(MAG.valueOf(matcher.group(2))){
            case K: return (long) Math.pow(2,10) * ord;
            case M: return (long) (Math.pow(2,20) * ord);
            case G: return (long) (Math.pow(2,30) * ord);
            default: throw new IllegalArgumentException("unparsable size given: "+ byteString);
        }
    }
}
