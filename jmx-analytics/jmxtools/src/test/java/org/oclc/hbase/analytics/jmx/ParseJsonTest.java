package org.oclc.hbase.analytics.jmx;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Stack;

import static org.junit.jupiter.api.Assertions.*;

public class ParseJsonTest {
    @Test
    void testSimpleParse(){
        try {
            String simpleJson = "{\"name\": \"cloudera\", \"version\": \"7.1.4\", \"license\": 2020}";
            JsonFactory jf = new JsonFactory();
            JsonParser jp = jf.createParser(simpleJson);

            String field = null;
            String value = null;
            while (jp.nextToken()!= JsonToken.END_OBJECT){
                System.out.println(jp.currentToken());
                if (jp.currentToken() == JsonToken.FIELD_NAME){
                    field = jp.currentName();
                    jp.nextToken();
                    value = jp.getText();
                    System.out.printf("f: %s, v: %s %n", field, value);
                }
            }
            jp.close();
            assertEquals("2020", value);
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testNestedObjects() {
        try {
            JsonFactory jf = new JsonFactory();
            JsonParser jp = jf.createParser(new File("src/test/resources/test1.json"));
            int numFields = 0;
            int depth = 1;
            Stack<String> currentField = new Stack<>();
            Stack<String> context = new Stack<>();
            context.push("root");
            jp.nextToken();
            if (jp.currentToken()!= JsonToken.START_OBJECT){
                throw new IllegalStateException("object is bad");
            }else {
                System.out.println("start object found");
            }
            jp.nextToken();
            while (depth > 0){
                if (jp.currentToken() == JsonToken.START_OBJECT){
                    depth++;
                }
                if (jp.currentToken()==JsonToken.END_OBJECT){
                    depth--;
                    currentField.pop();
                }
                if (jp.currentToken() == JsonToken.FIELD_NAME){
                    numFields++;
                    currentField.push(jp.getCurrentName());
                    System.out.printf("field: %s.%s(%d)%n",context.peek(), currentField.peek(), depth);
                }
                if (jp.currentToken() == JsonToken.START_ARRAY){
                    context.push(currentField.peek());
                }
                if (jp.currentToken()== JsonToken.END_ARRAY){
                    context.pop();
                }
                jp.nextToken();
            }
            jp.close();
            assertTrue(numFields == 7,"found 7 fields");
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }
}
