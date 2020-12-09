package org.oclc.hbase.tools.extractor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.json.JsonReadFeature;

import java.io.IOException;
import java.net.URL;
import java.util.Stack;

/**
 * extract all the 'name' fields from the json object
 */
public class ExtractsNamesFromJson {
    public static void main(String[] args) throws IOException {
        /*
        notes
        * we expect the object to contain a single element called 'beans'; an array.
         */
        JsonFactory jf = JsonFactory.builder()
                .configure(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS, true)
                .build();
        JsonParser jp = jf.createParser(new URL("http://hddev1db014dxc1.dev.oclc.org:60030/jmx"));
        int depth = 1;
        int fieldsDiscovered = 0;
        Stack<String> currentField = new Stack<>();
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
                fieldsDiscovered++;
                currentField.push(jp.getCurrentName());
//                System.out.printf("field: %s==%s (%d)%n",currentField.peek(), jp.nextTextValue(), depth);
                System.out.printf("%s%n", currentField.peek());
            }
            if (jp.currentToken() == JsonToken.START_ARRAY){
                depth++;
            }
            if (jp.currentToken()== JsonToken.END_ARRAY){
                depth--;
            }
            jp.nextToken();
        }
        jp.close();
        System.out.println("total fields discovered: " + fieldsDiscovered);
    }
}
