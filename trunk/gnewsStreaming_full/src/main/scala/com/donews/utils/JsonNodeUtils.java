package com.donews.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.ValidationMessage;


import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Set;

/**
 * Donews Company
 * Created by Liu dh
 * 2017/10/18.18:02
 */
public class JsonNodeUtils {

    public static JsonNode getJsonNodeFromStringContent(String content) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(content);
        return node;
    }

    public static JsonSchema getJsonSchemaFromStringContent(String schemaContent) throws Exception {
        JsonSchemaFactory factory = new JsonSchemaFactory();
        JsonSchema schema = factory.getSchema(schemaContent);
        return schema;
    }

    public static String validateData(ObjectNode node,String schema_str)throws Exception{
        JsonSchema schema = getJsonSchemaFromStringContent(schema_str);
        Set<ValidationMessage> messages = schema.validate(node);
        if(messages.size()>0){
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for(ValidationMessage msg:messages){
                sb.append(msg.getMessage()).append(",");
            }
            sb.append("}");
            return sb.toString();
        }else return "";

    }



}
