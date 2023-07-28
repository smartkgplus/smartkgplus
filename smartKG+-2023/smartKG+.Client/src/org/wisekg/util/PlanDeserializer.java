package org.wisekg.util;

import com.google.gson.*;
import org.wisekg.executionplan.QueryExecutionPlan;
import org.wisekg.executionplan.QueryOperator;
import org.rdfhdt.hdt.util.StarString;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class PlanDeserializer implements JsonDeserializer<QueryExecutionPlan> {
    @Override
    public QueryExecutionPlan deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
      //   System.out.println("deserialize");
        return deserializeSubplan(jsonElement);
    }

    private QueryExecutionPlan deserializeSubplan(JsonElement element) {
        
       
        JsonObject obj = element.getAsJsonObject();
       // System.out.println("deserializeSubplan ==>" + obj);
        if(!obj.has("operator") || !obj.has("subplan"))
        {
         //   System.out.println("I am finally done");
            return QueryExecutionPlan.getNullPlan();
        }
        
       // System.out.println("I still have more to do");
        return new QueryExecutionPlan(deserializeOperator(obj.get("operator")), deserializeSubplan(obj.get("subplan")), Long.parseLong(obj.get("timestamp").getAsString()));
    }

    private QueryOperator deserializeOperator(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        String control = obj.get("control").getAsString();
        //System.out.println("Control ==>" + control);
        return new QueryOperator(control, deserializeStar(obj.get("star")));
    }

    private StarString deserializeStar(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        String subject = obj.get("subject").getAsString();
        //System.out.println("subject ==>" + subject);
        return new StarString(subject, deserializeTriples(obj.getAsJsonArray("triples")));
    }

    private List<Tuple<CharSequence, CharSequence>> deserializeTriples(JsonArray arr) {
        List<Tuple<CharSequence, CharSequence>> lst = new ArrayList<>();
       
       // System.err.println("arr1 ==>" + arr.get(0));
       // System.err.println("arr2 ==>" + arr.get(1));
        int size = arr.size();
        // System.out.println("deserializeTriples ==>" + size);
        for(int i = 0; i < size; i++) {
           // System.out.println("i ==>" + i);
           // System.err.println("Triple: " + arr.get(i));
            lst.add(deserializeTriple(arr.get(i)));
        }
        //lst.forEach(x -> System.err.println("x: " + x.toString()));
        return lst;
    }

    private Tuple<CharSequence, CharSequence> deserializeTriple(JsonElement element) {
       // System.out.println("deserializeTriple" + element);
        JsonObject obj = element.getAsJsonObject();
       // System.out.println("deserialize+++Triple" + obj.get("x"));
        //  System.out.println("deserializeTriple ==>" + element.getAsString());
        return new Tuple<>(obj.get("x").getAsString(), obj.get("y").getAsString());
    }
}
