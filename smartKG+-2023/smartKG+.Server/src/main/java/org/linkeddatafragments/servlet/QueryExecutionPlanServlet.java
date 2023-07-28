package org.linkeddatafragments.servlet;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.http.HttpHeaders;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.Lang;
import org.linkeddatafragments.costmodel.ICostModel;
import org.linkeddatafragments.costmodel.impl.FamilyShippingCostModel;
import org.linkeddatafragments.costmodel.impl.StarCostModel;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.exceptions.DataSourceNotFoundException;
import org.linkeddatafragments.executionplan.QueryExecutionPlan;
import org.linkeddatafragments.executionplan.QueryOperator;
import org.linkeddatafragments.queryAnalyzer.Config;
import org.linkeddatafragments.queryAnalyzer.FamiliesConfig;
import org.linkeddatafragments.queryAnalyzer.Family;
import org.linkeddatafragments.util.BgpStarPatternVisitor;
import org.linkeddatafragments.util.MIMEParse;
import org.linkeddatafragments.util.StarString;
import org.linkeddatafragments.util.Tuple;
import org.linkeddatafragments.views.ILinkedDataFragmentWriter;
import org.linkeddatafragments.views.LinkedDataFragmentWriterFactory;
import org.rdfhdt.hdt.triples.TripleString;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import static java.lang.System.exit;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.jena.query.Syntax;
import org.apache.jena.shared.PrefixMapping;
import org.linkeddatafragments.datasource.DataSourceFactory;
import org.linkeddatafragments.datasource.DataSourceTypesRegistry;
import org.linkeddatafragments.datasource.IDataSourceType;

public class QueryExecutionPlanServlet extends HttpServlet {

    private ConfigReader config;
    private final static long serialVersionUID = 1L;
    private static HashMap<Node, List> familiesHashedByPredicate = null;
    private static long quantum = 300000L;

    // Parameters
    /**
     * baseURL
     */
    public final static String CFGFILE = "configFile";

    private final Collection<String> mimeTypes = new ArrayList<>();
    private final HashMap<String, IDataSource> dataSources = new HashMap<>();

    private File getConfigFile(ServletConfig config) throws IOException {

         System.out.println("Configuration in QueryExecutionPlanServlet");
        String path = config.getServletContext().getRealPath("/");
        
        System.out.println("Path:" + path);

        if (path == null) {
            path = System.getProperty("user.dir");
        }
        File cfg = new File("config-example.json");
        if (config.getInitParameter(CFGFILE) != null) {
            cfg = new File(config.getInitParameter(CFGFILE));
        }
        if (!cfg.exists()) {
            throw new IOException("Configuration file " + cfg + " not found.");
        }
        if (!cfg.isFile()) {
            throw new IOException("Configuration file " + cfg + " is not a file.");
        }
        return cfg;
    }

    /**
     * @param servletConfig
     * @throws ServletException
     */
    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
        try {
            
            System.out.println("I am in the init of Query Execution Plan");
            System.out.println("Servlet Config: " + servletConfig.toString());
            // load the configuration
            File configFile = getConfigFile(servletConfig);
            
             System.out.println("====>" + configFile.getAbsolutePath());
            PartitioningServlet.config = new ConfigReader(new FileReader(configFile));
            this.config = new ConfigReader(new FileReader(configFile));
            
           System.out.println("Config file loaded");
             for (Map.Entry<String, IDataSourceType> typeEntry : this.config.getDataSourceTypes().entrySet()) {
                DataSourceTypesRegistry.register(typeEntry.getKey(),
                        typeEntry.getValue());
            }
            System.out.println("getDataSourceTypes file loaded");
            // register data sources
            for (Map.Entry<String, JsonObject> dataSource : this.config.getDataSources().entrySet()) {
                
                //System.out.println("dataSource.getValue()=======>" + dataSource.getValue());
                IDataSource ds = DataSourceFactory.create(dataSource.getValue());
                //System.out.println("Factory DONE");
                dataSources.put(dataSource.getKey(), ds );
                //System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
            }
           // System.out.println("datasources file loaded");
            // register content types
            //MIMEParse.register("text/html");
            //MIMEParse.register(Lang.RDFXML.getHeaderString());
            //MIMEParse.register(Lang.NTRIPLES.getHeaderString());
            //MIMEParse.register(Lang.JSONLD.getHeaderString());
           // MIMEParse.register(Lang.TTL.getHeaderString());
            
            
            // register content types
            //MIMEParse.register("text/html");
            //MIMEParse.register(Lang.RDFXML.getHeaderString());
            //MIMEParse.register(Lang.NTRIPLES.getHeaderString());
            //MIMEParse.register(Lang.JSONLD.getHeaderString());
            MIMEParse.register(Lang.TTL.getHeaderString());
            //System.out.println("MIMEParse file loaded");
            // Called once
            Config config = new Config(configFile.getAbsolutePath());
            
            //System.out.println("get Data Sources" + this.config.getDataSources());
            familiesHashedByPredicate = FamiliesConfig.getInstance().getFamiliesByPredicate();
        } catch (Exception e) {
            
            System.out.print("We are in this exception");
            exit(0);
            throw new ServletException(e);
        }
    }

    /**
     *
     */
    @Override
    public void destroy() {
    }

    /**
     * @param request
     * @param response
     * @throws ServletException
     */
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException {
        try {

            //System.out.println("Welcome to the query plan do get");
            String acceptHeader = request.getHeader(HttpHeaders.ACCEPT);
            String bestMatch = MIMEParse.bestMatch(acceptHeader);

            response.setHeader(HttpHeaders.SERVER, "Linked Data Fragments Server");
            response.setContentType(bestMatch);
            response.setCharacterEncoding(StandardCharsets.UTF_8.name());

            String bgpString = request.getParameter("bgp");
            if (bgpString == null) {
                throw new ServletException("BGP not specified.");
            }

            String qStr = "SELECT * WHERE { " + bgpString + " }";

            //System.out.println("qStr ==>" + qStr);
            Query query = QueryFactory.create(qStr);

            BgpStarPatternVisitor visitor = new BgpStarPatternVisitor();
            query.getQueryPattern().visit(visitor);

            List<StarString> stars = new ArrayList<>(visitor.getStarPatterns());

            //System.out.println("before createExecutionPlan");
            long s = System.currentTimeMillis();
            QueryExecutionPlan plan = createExecutionPlan(stars, QueryExecutionPlan.getNullPlan(), request, new ArrayList<>());
             //System.out.println("After createExecutionPlan" + s);
            Gson gson = new Gson();

            response.getWriter().println(gson.toJson(plan));
           // System.out.println("plan is ready");
            System.gc();
        } catch (Exception e) {
            
            System.out.println("I am in thr exception of THE PLAN SERVLET");
            throw new ServletException(e);
        }
    }

    private StarString getFirstStar(List<StarString> stars) throws FileNotFoundException {
        
        //System.out.println("getFirstStar: " + stars.size());
        if (stars.size() == 0) {
            return null;
        }
        if (stars.size() == 1) {
            return stars.get(0);
        }

        StarString best = null;
        long low = Long.MAX_VALUE;
        //System.out.println("LOW: " + low);
        for (StarString star : stars) {
        
            IDataSource ds;
            if (star.size() == 1 || hasInfrequent(star)) {
               // System.out.println("before plan single star");
                ds = dataSources.get(this.config.getDefaultGraph());
               // System.out.println("After plan single star");
                //System.out.println("plan single star with card" + ds.cardinalityEstimation(star));
            } else {
                //System.out.println("plan family partition ");
                int family = getQueryStarsFamilies(star).get(0);
                try {
                  //  System.out.println("getDataSource from Plan Servlet: " + PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt");
                    ds = PartitioningServlet.getDataSource(PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt");

                //    System.out.println("ds ==>" + ds);
                } catch (DataSourceNotFoundException e) {
                    continue;
                }
            }
            //System.out.println("choosing best: " + star);
            long card = ds.cardinalityEstimation(star);
            //System.out.println("star: " + star);
            //System.out.println("Card: " + card);
            //if(card == 0) return null;
            if (card < low) {
                low = card;
                best = star;
            }
        }

        return best;
    }

    private StarString getNextStar(List<StarString> stars, List<String> boundVars) {
        //System.out.println("getNextStar: " + stars.size());
        if (stars.size() == 0) {
            return null;
        }
        if (stars.size() == 1) {
            //System.out.println("only one star: " + stars.get(0));
            return stars.get(0);
        }

        StarString best = null;
        int max = -1;
        int maxBSO = -1;
        for (StarString star : stars) {
            int bv = star.numBoundVars(boundVars);
            int bso = star.numBoundSO(boundVars);
            if (bv > max || (bv == max && bso > maxBSO)) {
                max = bv;
                maxBSO = bso;
                best = star;
            }
        }
        
        //System.out.println("best: " + best);
        return best;
    }

    private QueryExecutionPlan createExecutionPlan(List<StarString> stars, QueryExecutionPlan subplan, HttpServletRequest request, List<String> boundVars) throws FileNotFoundException, DataSourceNotFoundException {
        if (stars.size() == 0) {
            return subplan;
        }
        StarString next = null;

          //System.out.println(stars.toString());
        if (subplan.isNullPlan()) {
            next = getFirstStar(stars);
        } else {
            next = getNextStar(stars, boundVars);
        }

        if (next == null) {
            return subplan;
        }

         //System.out.println(next.toString());
        boundVars.addAll(next.getVariables());

        int family = 0;
        IDataSource datasource = null;
        String partitionUrl;
        if (next.size() == 1 || hasInfrequent(next)) {
              //System.out.println("next.size() == 1 || hasInfrequent(next)");
              //System.out.println(PartitioningServlet.config.getUri() + PartitioningServlet.config.getDefaultGraph());
            QueryOperator operator = new QueryOperator(PartitioningServlet.config.getUri() + PartitioningServlet.config.getDefaultGraph(), next);
            List<StarString> lst = new ArrayList<>(stars);
            lst.remove(next);
            QueryExecutionPlan p = new QueryExecutionPlan(operator, subplan, System.currentTimeMillis() + quantum);
            return createExecutionPlan(lst, p, request, boundVars);
        } else {
          //  try {
                //System.out.println("In the else of next.size() == 1 || hasInfrequent(next)");
                family = getQueryStarsFamilies(next).get(0);
             //   System.out.println("family: " + family);
                partitionUrl = PartitioningServlet.config.getUri() + "partition/" + PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt";
               System.out.println("===> partitionUrl:" + partitionUrl);
             
               System.out.println("check if the input is not " + PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt");
                
               
               if(next.isHasType() && !next.getClassValue().contains("?")){
                   System.out.println("***inside the f");
                    datasource = PartitioningServlet.getDataSource(PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt_" + next.getClassValue() + ".hdt");
                    System.out.println("***datasource: " + datasource.getFilename());
                    if (datasource == null) datasource = PartitioningServlet.getDataSource(PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt");
               }else{
                    datasource = PartitioningServlet.getDataSource(PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt");
               }
        
              
               // System.out.println("I did not reach this part");
            //} catch (DataSourceNotFoundException e) {
             //   System.out.println("Error getting datasource/family");
              //  return QueryExecutionPlan.getNullPlan();
          //  }
        }
        
        

        final ICostModel shippingCostModel = new FamilyShippingCostModel(PartitioningServlet.config, next, subplan);
        
       
        final ICostModel localCostModel = new StarCostModel(next, subplan);

         double shipping = shippingCostModel.cost(request, datasource); 
         System.out.println("shippingCostModel: " + shipping);
        System.err.println("data source" + datasource);
       // double shipping = 1000;
        double local = localCostModel.cost(request, datasource);
        //double local = 10000;
         System.out.println("localCostModel: " + local);
        //double local = 1;
        QueryOperator op;
        if (local < shipping) {
            op = new QueryOperator(partitionUrl, next);
            // System.err.println("local ===>" + op.getStar() + "Control ==>" + op.getControl());
        } else {
             //System.out.println("shipping before operator ===>" + PartitioningServlet.config.getUri() + "molecule/" + PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt");
            //op = new QueryOperator(PartitioningServlet.config.getUri() + "molecule/" + PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt", next);
            //System.out.println("getTheStar===>" + op.getStar());
             System.err.println("next.isHasType():" + next.isHasType() + "next.getClassValue():" + next.getClassValue());
            
             if(next.isHasType() && !next.getClassValue().contains("?")){
                System.err.println("getUri(): " + PartitioningServlet.config.getUri() );
                System.err.println("getPartitionstring: " + PartitioningServlet.config.getPartitionstring());
                System.err.println("Went inside getClass");
                System.err.println("QueryOperator: =>" + PartitioningServlet.config.getUri() + "molecule/" + PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt_" + next.getClassValue() + ".hdt");
                
                op = new QueryOperator(PartitioningServlet.config.getUri() + "molecule/" + PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt_" + next.getClassValue() + ".hdt", next);
            }else{
                System.err.println("Went inside normal partitioning");
                op = new QueryOperator(PartitioningServlet.config.getUri() + "molecule/" + PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt", next);
            }
            
        }

        QueryExecutionPlan p = new QueryExecutionPlan(op, subplan, System.currentTimeMillis() + quantum);
        List<StarString> lst = new ArrayList<>(stars);
        lst.remove(next);
        return createExecutionPlan(lst, p, request, boundVars);
    }

    private boolean hasInfrequent(StarString sp) {
        for (Tuple<CharSequence, CharSequence> stmt : sp.getTriples()) {
            if (!familiesHashedByPredicate.containsKey(NodeFactory.createURI(stmt.x.toString()))) {
                return true;
            }
        }
        return false;
    }

    private ArrayList<Integer> intersection(List<Integer> starQueryFamilies, List<Integer> tripleFamilyList) {
        // System.out.print("starQueryFamilies:" + starQueryFamilies.size());
         // System.out.print("tripleFamilyList:" + tripleFamilyList.size());
        //System.out.println("==dsds==");
        if (tripleFamilyList == null) {
            return new ArrayList<>(starQueryFamilies);
        }
        int i = 0;
        int j = 0;
        ArrayList<Integer> intersectedList = new ArrayList<>();
        while (i < starQueryFamilies.size() && j < tripleFamilyList.size()) {
            // System.out.println("==tripleFamilyList==");
            if (starQueryFamilies.get(i) < tripleFamilyList.get(j)) {
                i++;// Increase I move to next element
            } else if (tripleFamilyList.get(j) < starQueryFamilies.get(i)) {
                j++;// Increase J move to next element
            } else {
                intersectedList.add(tripleFamilyList.get(j));
                i++;// If same increase I & J both
            }
        }
         //System.out.println("intersectedList: " + intersectedList.size());
        return intersectedList;
    }

    private List<Integer> getQueryStarsFamilies(StarString starPattern) {
        //todo Change if we need receive multiple files per star patterns
        List<Integer> queryStarsFamilies = new ArrayList<>();
        TripleString stmtPattern = starPattern.getTriple(0);
        List<Integer> starQueryFamilies = familiesHashedByPredicate.
                get(NodeFactory.createURI(stmtPattern.getPredicate().toString()));

        while (starQueryFamilies == null) {
            if (starPattern.size() == 1) {
                return queryStarsFamilies;
            }
            starPattern = new StarString(stmtPattern.getSubject(), starPattern.getTriples().subList(1, starPattern.getTriples().size()));
            stmtPattern = starPattern.getTriple(0);
            starQueryFamilies = familiesHashedByPredicate.
                    get(NodeFactory.createURI(stmtPattern.getPredicate().toString()));
        }
        if (starQueryFamilies != null) {
            int size = starPattern.size();
            for (int i = 1; i < size; i++) {
                TripleString stmt = starPattern.getTriple(i);
                starQueryFamilies = intersection(starQueryFamilies,
                        familiesHashedByPredicate.get(NodeFactory.createURI(stmt.getPredicate().toString())));
            }
            
           // System.out.println("finished intersection and start grouping");
            List<Integer> starQueryGroupedFamilies = starQueryFamilies.stream().filter(familyId
                    -> FamiliesConfig.getInstance().getFamilyByID(familyId).isGrouped()).collect(Collectors.toList());
            if (starQueryGroupedFamilies.isEmpty()) {
                queryStarsFamilies.addAll(starQueryFamilies);
            } else if (starQueryGroupedFamilies.size() == 1) {
                queryStarsFamilies.addAll(starQueryGroupedFamilies);
            } else if (starQueryGroupedFamilies.size() > 1) {
                Optional<Integer> PartitionId = starQueryGroupedFamilies.stream().min((f1, f2) -> Integer.compare(
                        FamiliesConfig.getInstance().getFamilyByID(f1).getPredicateSet().size(),
                        FamiliesConfig.getInstance().getFamilyByID(f2).getPredicateSet().size()));
                Family familySolution = FamiliesConfig.getInstance().getFamilyByID(PartitionId.get());

                List<Integer> sourceSet = familySolution.getSourceSet();

                if (sourceSet != null) {
                    if (familySolution.isOriginalFamily()) {
                        sourceSet.add(familySolution.getIndex());
                    }
                    queryStarsFamilies.addAll(sourceSet);
                } else {
                    queryStarsFamilies.addAll(new ArrayList<>(Arrays.asList(PartitionId.get())));
                }
            } else {
                // Todo remove this part
                //System.out.println("1 predicate star");
                List<Integer> starQueryDetailedFamilies = starQueryFamilies.stream().filter(
                        familyId -> !(FamiliesConfig.getInstance().getFamilyByID(familyId)).isGrouped()).collect(Collectors.toList());
                queryStarsFamilies.addAll(starQueryDetailedFamilies);
            }
        }
        
       // System.out.println("grouped familes are ready");
        return queryStarsFamilies;
    }
}
