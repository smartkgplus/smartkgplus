package org.wisekg.callable;

import java.io.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.wisekg.executionplan.QueryExecutionPlan;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.wisekg.main.SparqlQueryProcessor;
import org.wisekg.task.InitialHttpRequestTask;
import org.wisekg.util.PlanDeserializer;

public class InitialHttpRequestThread implements Callable<QueryExecutionPlan> {
    private InitialHttpRequestTask httpRequestTask;
    private ConcurrentHashMap<String, Content> httpResponseCache;

    public InitialHttpRequestThread(InitialHttpRequestTask httpRequestTask, ConcurrentHashMap<String, Content> httpResponseCache) {
        this.httpRequestTask = httpRequestTask;
        this.httpResponseCache = httpResponseCache;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public QueryExecutionPlan call() throws Exception {
        QueryExecutionPlan plan = QueryExecutionPlan.getNullPlan();
        try {
            String httpUrl = httpRequestTask.getFragmentURL();
            Content content = null;
            boolean cacheContains = false;
             
            if (httpResponseCache.containsKey(httpUrl)) {
                cacheContains = true;
                content = httpResponseCache.get(httpUrl);
            } else {
                SparqlQueryProcessor.NUMBER_OF_HTTP_REQUESTS.incrementAndGet();
                SparqlQueryProcessor.SERVER_REQUESTS.incrementAndGet();
                // System.out.println("start QueryExecutionPlan");
                content = Request.Get(httpUrl).addHeader("accept", "text/turtle").execute().returnContent();
                //System.out.println("QueryExecutionPlan TRANSFERRED_BYTES:" + content.asBytes().length + httpUrl.getBytes().length);
                SparqlQueryProcessor.TRANSFERRED_BYTES.addAndGet(content.asBytes().length + httpUrl.getBytes().length);
            }

            GsonBuilder gs = new GsonBuilder();
            gs.registerTypeAdapter(QueryExecutionPlan.class, new PlanDeserializer());
         //   System.out.println("Gso0o0o0on");
            Gson gson = gs.create();
            //System.out.println("Gso0o0o0on");
           // System.out.println("content ===>" + content.asString());
            plan = gson.fromJson(content.asString(), QueryExecutionPlan.class);
         //   System.out.println("Gso04567o0o0on");
            
       
            if (!cacheContains) {
                if(httpResponseCache.size() == SparqlQueryProcessor.MAX_CACHE_ENTRIES) {
                    httpResponseCache.remove(SparqlQueryProcessor.cacheQueue.poll());
                }
                httpResponseCache.put(httpUrl, content);
                SparqlQueryProcessor.cacheQueue.add(httpUrl);
            }
            
         
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("There was an exception");
        }
        return plan;
    }
}
