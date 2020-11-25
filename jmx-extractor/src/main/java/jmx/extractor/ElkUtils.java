package jmx.extractor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import jmx.extractor.model.MetaField;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class ElkUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ElkUtils.class);
    private static ObjectMapper om = new ObjectMapper();

    public static void post(URI elkUri, Map<String, Object> map) throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        String json = om.writeValueAsString(map);
        // use httpclient to sent it to elk
        Response response = Request.Post(elkUri)
                .bodyString(json, ContentType.APPLICATION_JSON)
                .execute();
        int statusCode = response.returnResponse().getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_CREATED){
            LOG.warn("send to elasticsearch failed; rc={}", statusCode);
            LOG.warn("http error was: {}", response.returnContent().asString());
        }
        sw.stop();
        LOG.debug("POST->{}, elapsed({})", elkUri, sw.toString());

    }
}
