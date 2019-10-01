package org.oclc.hadoop.perf.mr;


import org.apache.hadoop.hbase.TableName;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.*;

public class AssumptionTest {
    @Test
    public void testTableName(){
        String strTable = "Worldcat";
        String strNsTable = "default:Worldcat";
        TableName tn1 = TableName.valueOf(strTable);
        assertThat(tn1.getQualifierAsString(),is(strTable));
        assertThat("we expect a default NS", tn1.getNamespaceAsString(), is("default"));
        TableName tn2 = TableName.valueOf(strNsTable);
        assertThat(tn2.getQualifierAsString(),is(strTable));
        assertThat(tn2.getNamespaceAsString(),is("default"));

    }
}
