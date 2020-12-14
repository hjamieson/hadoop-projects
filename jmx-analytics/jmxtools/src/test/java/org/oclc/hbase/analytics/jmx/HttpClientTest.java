package org.oclc.hbase.analytics.jmx;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * these tests expect elasticsearch to be listening on localhost:9200!
 */
public class HttpClientTest {
    @Test
    void testHttpGet() {
        try {
            String response = Request.Get("http://localhost:9200")
                    .socketTimeout(1000)
                    .execute()
                    .returnContent()
                    .asString();
            System.out.printf("resp: %s%n", response);
            assertTrue(response.contains("for Search"));
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    @Test
    void testPutDoc(){
        try {
            int rc = Request.Post("http://localhost:9200/test/_doc")
                    .socketTimeout(1000)
                    .bodyFile(new File("src/test/resources/test1.json"), ContentType.APPLICATION_JSON)
                    .execute()
                    .returnResponse().getStatusLine().getStatusCode();
            assertEquals(201, rc);
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
