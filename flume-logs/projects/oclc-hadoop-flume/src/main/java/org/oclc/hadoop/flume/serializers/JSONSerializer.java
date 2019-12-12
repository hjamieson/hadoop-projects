package org.oclc.hadoop.flume.serializers;

import com.google.gson.Gson;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

class JSONSerializer implements EventSerializer {
    public static final String APPEND_NL = "jsonserializer_append_nl";
    private final Gson gson;
    private Context ctx;
    private OutputStream os;
    private boolean appendNL = true;

    @Override
    public void afterCreate() throws IOException {
        // noop
    }

    @Override
    public void afterReopen() throws IOException {
        //noop
    }

    @Override
    public void write(Event event) throws IOException {
        os.write(eventAsJSON(event).getBytes());
    }

    private String eventAsJSON(Event event) {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, String> header : event.getHeaders().entrySet()) {
            map.put(header.getKey(), header.getValue());
        }
        map.put("data", new String(event.getBody()));
        return gson.toJson(map);
    }

    @Override
    public void flush() throws IOException {
        os.flush();
    }

    @Override
    public void beforeClose() throws IOException {
        os.flush();
    }

    @Override
    public boolean supportsReopen() {
        return false;
    }

    private JSONSerializer(Context ctx, OutputStream os) {
        this.ctx = ctx;
        this.os = os;
        gson = new Gson();
        this.appendNL = ctx.getBoolean(APPEND_NL, true);
    }

    public static class Builder implements EventSerializer.Builder {
        @Override
        public EventSerializer build(Context context, OutputStream out) {
            return new JSONSerializer(context, out);
        }
    }
}
