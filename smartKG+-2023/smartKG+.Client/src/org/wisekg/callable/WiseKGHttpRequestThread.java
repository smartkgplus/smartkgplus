package org.wisekg.callable;

import java.io.FileOutputStream;
import org.wisekg.main.Experiment;
import org.wisekg.main.SparqlQueryProcessor;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.wisekg.task.HdtParseResponseTask;
import org.wisekg.task.InitialHttpRequestTask;
import org.wisekg.task.WiseKGHttpRequestTask;
import org.wisekg.task.WiseKGParseResponseTask;
import org.wisekg.util.Config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;

public class WiseKGHttpRequestThread implements Callable<Boolean> {

    private WiseKGHttpRequestTask httpRequestTask;
    private ConcurrentHashMap<String, Content> httpResponseCache;
    private ConcurrentHashMap<String, HDT> hdtResponseCache;
    private ExecutorCompletionService<Boolean> executorCompletionService;
    private AtomicInteger numberOfTasks;
    public static int currtpIdx = 0;
    public static boolean started = false;
    public static long startTime = 0;
    public static String currResStr = "";
    public static long totalBindings = 0;
    public static String currMethod = "";

    public WiseKGHttpRequestThread(WiseKGHttpRequestTask httpRequestTask,
            ConcurrentHashMap<String, Content> httpResponseCache,
            ConcurrentHashMap<String, HDT> hdtResponseCache,
            ExecutorCompletionService<Boolean> executorCompletionService, AtomicInteger numberOfTasks) {
        this.httpRequestTask = httpRequestTask;
        this.httpResponseCache = httpResponseCache;
        this.hdtResponseCache = hdtResponseCache;
        this.executorCompletionService = executorCompletionService;
        this.numberOfTasks = numberOfTasks;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public Boolean call() throws IOException {
        if (httpRequestTask.getPlan().getTimestamp() < System.currentTimeMillis()) {
            InitialHttpRequestTask task = new InitialHttpRequestTask(httpRequestTask.getPlan().getTriples());
            InitialHttpRequestThread thread = new InitialHttpRequestThread(task, httpResponseCache);
            try {
                httpRequestTask.setPlan(thread.call());
            } catch (Exception e) {
            }
        }

        List<String> keyslist = new ArrayList<>();
        String control = httpRequestTask.getOperator().getControl();
        //System.out.println("Control: " + control);
        //String filePathString = Config.getInstance().getDownloadedpartitions() + control.substring(control.lastIndexOf("/") + 1);
        String filePathString = control.substring(control.lastIndexOf("/") + 1);
        //System.out.println("filePathString: ==> " + filePathString);

        if (hdtResponseCache.containsKey(filePathString)) {
            //System.out.println("yes we have already cached this file");
            //currMethod = "SMARTKG";
            HdtParseResponseTask prTask = new HdtParseResponseTask(httpRequestTask);
            numberOfTasks.incrementAndGet();
            HdtResponseParserThread rpThread = new HdtResponseParserThread(prTask, executorCompletionService, httpResponseCache,
                    hdtResponseCache, numberOfTasks, httpRequestTask.getStarPattern(), hdtResponseCache.get(filePathString));
            executorCompletionService.submit(rpThread);
        } else {
            String httpUrl = httpRequestTask.getFragmentURL();

            Content content = null;
            if (httpResponseCache.containsKey(httpUrl)) {
                content = httpResponseCache.get(httpUrl);
            } else {
                SparqlQueryProcessor.NUMBER_OF_HTTP_REQUESTS.incrementAndGet();
                SparqlQueryProcessor.NUMBER_OF_BINDINGS_SENT.addAndGet(httpRequestTask.getBindings().size());

                SparqlQueryProcessor.SERVER_REQUESTS.incrementAndGet();
                content = Request.Get(httpUrl).addHeader("accept", "text/turtle").execute().returnContent();
              // System.out.println("TRANSFERRED_BYTES:" + content.asBytes().length + httpUrl.getBytes().length);
                SparqlQueryProcessor.TRANSFERRED_BYTES.addAndGet(content.asBytes().length + httpUrl.getBytes().length);

                httpResponseCache.put(httpUrl, content);
            }
            if (content != null) {
                if (content.asString().contains("$HDT")) {

                  //  System.out.println("SMARTKG ==> start downloading");
                    //currMethod = "SMARTKG";
                    //TarArchiveInputStream tarInput = (TarArchiveInputStream) ;
                    InputStream hdtfis = content.asStream();
                    // saveHdtFile(filePathString, fis);
                    TarArchiveInputStream tis = new TarArchiveInputStream(hdtfis);
                    keyslist = saveHdtFilefromTar(tis);
                    //System.out.println("keyslist ===> " + keyslist);

                    String httpUrlIndex = httpRequestTask.getFragmentURL().replace(".hdt", ".hdt.index.v1-1");
                    //Content contentIndex = Request.Get(httpUrlIndex).addHeader("accept", "text/turtle").execute().returnContent();
                    //InputStream inputStreamIndex = contentIndex.asStream();
                    //   String filePathStringIndex = filePathString.replace(".hdt", ".hdt.index.v1-1");
                    //saveHdtFile(filePathStringIndex, inputStreamIndex);
                    //TarArchiveInputStream indexfis = new TarArchiveInputStream(inputStreamIndex);
                    //saveIndexFilefromTar(indexfis);

                    hdtfis.close();
                    tis.close();
                    ///indexfis.close();
                    // System.out.println("filePathStringIndex: " + filePathStringIndex);

                    /* try {
                        System.out.println("hdtResponseCache: " + hdtResponseCache);
                        System.out.println("before filePathString: " + filePathString);
                        hdtResponseCache.put(filePathString, HDTManager.mapIndexedHDT(filePathString));
                        System.out.println("after filePathString: " + filePathString);
                    } catch (IllegalFormatException e) {
                        throw new HdtException("Error for client " + Experiment.CLIENT_NUM + " query " + Experiment.QUERY);
                    }*/
                    //System.out.println("filePathString====>" + filePathString);
                    //System.out.println("hdtResponseCache====>" + hdtResponseCache);
                    //String key = filePathString.substring(filePathString.lastIndexOf("/") + 1);
                    //System.out.println("keyslist====>" + keyslist);
                 //   for (String key : keyslist) {
                        //System.out.println("key====>" + key);
                        // System.out.println("hdtResponseCache.get====>" + hdtResponseCache.get(key));
                        HdtParseResponseTask prTask = new HdtParseResponseTask(httpRequestTask);
                        numberOfTasks.incrementAndGet();
                        HdtResponseParserThread rpThread = new HdtResponseParserThread(prTask, executorCompletionService, httpResponseCache,
                                hdtResponseCache, numberOfTasks, httpRequestTask.getStarPattern(), hdtResponseCache.get(keyslist.get(0)));
                        //System.out.println("after initializing HdtResponseParserThread");
                        executorCompletionService.submit(rpThread);
                   // }

                } else {
                   // System.out.println("===> no $HDT");
                    WiseKGParseResponseTask prTask = new WiseKGParseResponseTask(httpRequestTask, content.asStream());
                    numberOfTasks.incrementAndGet();
                    WiseKGResponseParserThread rpThread = new WiseKGResponseParserThread(prTask, executorCompletionService,
                            httpResponseCache, hdtResponseCache, numberOfTasks);
                    executorCompletionService.submit(rpThread);
                }
            }
        }
        return true;
    }

    private class HdtException extends RuntimeException {

        public HdtException(String msg) {
            super(msg);
        }
    }

    private List<String> saveHdtFilefromTar(TarArchiveInputStream tis) throws IOException {

        List<String> keyslist = new ArrayList();
        TarArchiveEntry TarEntryHDT =  tis.getNextTarEntry();
        FileOutputStream outHDT = new FileOutputStream(Config.getInstance().getDownloadedpartitions() + TarEntryHDT.getName());
        IOUtils.copy(tis, outHDT);
        
        TarArchiveEntry TarEntryIndex = tis.getNextTarEntry();
        FileOutputStream outIndex = new FileOutputStream(Config.getInstance().getDownloadedpartitions() + TarEntryIndex.getName());
        IOUtils.copy(tis, outIndex);
        
        //System.out.println("TarEntryHDT: " + TarEntryHDT.getName());
        //System.out.println("TarEntryIndex: " + TarEntryIndex.getName());
        keyslist.add(TarEntryHDT.getName());
        hdtResponseCache.put(TarEntryHDT.getName(), HDTManager.loadIndexedHDT(Config.getInstance().getDownloadedpartitions() + TarEntryHDT.getName()));
        
        
        //System.out.println("record size: " + tis.getRecordSize());
      /*  while ((tarEntry = tis.getNextTarEntry()) != null) {
            String name = tarEntry.getName();
            //System.out.println("Entry ==> " + name);
            //System.out.println("before outStream ==> " + Config.getInstance().getDownloadedpartitions() + name);
            FileOutputStream out = new FileOutputStream(Config.getInstance().getDownloadedpartitions() + name);
            IOUtils.copy(tis, out);
            //System.out.println("after copying is done");
            if (!name.contains("index")) {
                //System.out.println("NAME ==>" + name);
                keyslist.add(name);
                hdtResponseCache.put(name, HDTManager.loadIndexedHDT(Config.getInstance().getDownloadedpartitions() + name));
            }
      
            out.close();
            //System.out.println("caching is done" + keyslist);
        } */
        outHDT.close();
        outIndex.close();
        tis.close();
        return keyslist;
    }

    private void saveIndexFilefromTar(TarArchiveInputStream indexfis) throws IOException {
        TarArchiveEntry tarEntry = null;

        //System.out.println("record size: " + indexfis.getRecordSize());
        while ((tarEntry = indexfis.getNextTarEntry()) != null) {
            String name = tarEntry.getName();
            //before filePathString: downloadedPartitions/client1/part_watdiv.10M_2974.hdt
            //System.out.println("Entry ==> " + name);
            //System.out.println("before copying is done ==> " + Config.getInstance().getDownloadedpartitions() + name);
            FileOutputStream out = new FileOutputStream(Config.getInstance().getDownloadedpartitions() + name);
            IOUtils.copy(indexfis, out);
            //System.out.println("after copying is done");
            if (!name.contains("index")) {

                //HDTManager.mapIndexedHDT(Config.getInstance().getDownloadedpartitions() + name);
                //HDT x =   
                hdtResponseCache.put(name, HDTManager.mapIndexedHDT(Config.getInstance().getDownloadedpartitions() + name));
            }
            out.close();
        }

        indexfis.close();
    }

    private void saveHdtFile(String path, InputStream stream) throws IOException {

        //System.out.println("b ==>" + path);
        //System.out.println("Path ==>" + Paths.get(path));
        Files.copy(stream, Paths.get(path), StandardCopyOption.REPLACE_EXISTING);
        stream.close();
    }
}
