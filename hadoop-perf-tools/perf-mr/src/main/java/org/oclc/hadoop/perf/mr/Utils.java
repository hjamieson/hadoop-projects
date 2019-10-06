package org.oclc.hadoop.perf.mr;

import org.apache.commons.lang.RandomStringUtils;

import java.net.URL;
import java.util.Date;
import java.util.Enumeration;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Any shared utility methods can be found here.
 */
public class Utils {
    public static final int RANDOM_TEXT_LEN = 128;
    public static String rgx = "(\\d+)(K|M|G)";
    private static Pattern pat = Pattern.compile(rgx);

    enum MAG {K, M, G}

    public enum CONTENT {TEXT, JSON, XML, RANDOMTEXT }

    private static Random rand = new Random(new Date().getTime());

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
        switch (MAG.valueOf(matcher.group(2))) {
            case K:
                return (long) Math.pow(2, 10) * ord;
            case M:
                return (long) (Math.pow(2, 20) * ord);
            case G:
                return (long) (Math.pow(2, 30) * ord);
            default:
                throw new IllegalArgumentException("unparsable size given: " + byteString);
        }
    }

    public static byte[] randomContent(CONTENT content) {
        switch (content) {
            case TEXT:
                return randomText[rand.nextInt(randomText.length)].getBytes();
            case JSON:
                return randomJson[rand.nextInt(randomJson.length)].getBytes();
            case XML:
                return randomXml[rand.nextInt(randomXml.length)].getBytes();
            case RANDOMTEXT:
                return RandomStringUtils.randomAlphanumeric(RANDOM_TEXT_LEN).getBytes();
        }
        return new byte[0];
    }

    private static String[] randomText = new String[]{
            "Ohio",
            "Texas",
            "Washington",
            "Tokyo"
    };

    private static String[] randomJson = new String[]{
        "{\"name\":\"john\",\"age\":25}"
    };

    private static String[] randomXml = new String[]{
      "<root><child></child></root>"
    };

    public static void locateClass(Class myClass, ClassLoader _loader){
        ClassLoader loader = _loader == null ? myClass.getClassLoader(): _loader;
        String classFile = myClass.getName().replaceAll("\\.", "/") + ".class";
        System.out.format("*** class: %s\n", classFile);
        try {
            Enumeration<URL> resources = loader.getResources(classFile);
            while (resources.hasMoreElements()){
                URL url = resources.nextElement();
                System.out.format("url=%s%n",url);
            }
            Class<?> aClass = Class.forName(myClass.getName(), true, loader);
            System.out.println("class was loaded successfully");
            System.out.println("***");
        } catch (Exception ex){
            ex.printStackTrace();
        }

    }
}
