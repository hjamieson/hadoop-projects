package org.oclc.hadoop.perf.mr;


import org.apache.hadoop.hbase.TableName;
import org.junit.Test;

import java.net.URL;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.*;

public class AssumptionTest {
    @Test
    public void testTableName() {
        String strTable = "Worldcat";
        String strNsTable = "default:Worldcat";
        TableName tn1 = TableName.valueOf(strTable);
        assertThat(tn1.getQualifierAsString(), is(strTable));
        assertThat("we expect a default NS", tn1.getNamespaceAsString(), is("default"));
        TableName tn2 = TableName.valueOf(strNsTable);
        assertThat(tn2.getQualifierAsString(), is(strTable));
        assertThat(tn2.getNamespaceAsString(), is("default"));

    }

    @Test
    public void testSizeCalc() {
        String rgx = "(\\d+)(K|M|G)";
        Pattern pat = Pattern.compile(rgx);
        assertThat("10G".matches(rgx), is(true));
        assertThat("10K".matches(rgx), is(true));
        assertThat("10M".matches(rgx), is(true));
        assertThat("10m".matches(rgx), is(false));
        assertThat(pat.matcher("25M").matches(), is(true));
        Matcher m1 = pat.matcher("125M");
        assertThat(m1.matches(), is(true));
        assertThat(m1.group(1), is("125"));
        assertThat(m1.group(2), is("M"));
    }

    @Test
    public void testClassLoader(){
        Class myClass = org.oclc.hadoop.perf.mr.inputformat.FakeInputFormat.class;
        ClassLoader loader = myClass.getClassLoader();
        String classFile = myClass.getName().replaceAll("\\.", "/") + ".class";
        System.out.println(classFile);
        try {
            Enumeration<URL> resources = loader.getResources(classFile);
            while (resources.hasMoreElements()){
                URL url = resources.nextElement();
                System.out.println(url);
            }
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }

    @Test
    public void modulusTest(){
        int max = 500;
        for (int i = 0; i< max; i++){
            if ( i % 100 == 0){
                System.out.println(i);
            }
        }
    }
}
