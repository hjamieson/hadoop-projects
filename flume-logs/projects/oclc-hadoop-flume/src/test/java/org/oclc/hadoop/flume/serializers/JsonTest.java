package org.oclc.hadoop.flume.serializers;


import com.google.gson.Gson;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class JsonTest {

    private Gson gson;

    @Before
    public void setup(){
        gson = new Gson();

    }

    @Test
    public void simpleTest(){
        int one = gson.fromJson("1", int.class);
        assertThat(one, is(1));
        assertThat(gson.toJson("abcd"), is("\"abcd\""));
    }

    @Test
    public void testMap(){
        Map<String,String> map = new HashMap<>();
        map.put("name","hugh");
        map.put("last","jamieson");
        map.put("empno","6279");
        String json = gson.toJson(map);
        System.out.println(json);
        assertThat(json, containsString("\"name\""));
    }
}
