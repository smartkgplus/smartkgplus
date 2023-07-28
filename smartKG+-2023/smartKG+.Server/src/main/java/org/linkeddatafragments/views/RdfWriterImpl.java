package org.linkeddatafragments.views;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;

import org.apache.jena.query.ARQ;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.fragments.spf.IStarPatternFragment;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragment;


class RdfWriterImpl extends LinkedDataFragmentWriterBase implements ILinkedDataFragmentWriter {

    private final Lang contentType;

    public RdfWriterImpl(Map<String, String> prefixes, HashMap<String, IDataSource> datasources, String mimeType) {
        super(prefixes, datasources);
        this.contentType = RDFLanguages.contentTypeToLang(mimeType);
        ARQ.init();
    }

    @Override
    public void writeNotFound(ServletOutputStream outputStream, HttpServletRequest request) throws IOException {
        outputStream.println(request.getRequestURL().toString() + " not found!");
        outputStream.close();
    }

    @Override
    public void writeError(ServletOutputStream outputStream, Exception ex) throws IOException {
        outputStream.println(ex.getMessage());
        outputStream.close();
    }

    @Override
    public void writeFragment(OutputStream outputStream, IDataSource datasource, ILinkedDataFragment fragment, ILinkedDataFragmentRequest ldfRequest) throws Exception {
        if(fragment instanceof ITriplePatternFragment) {
            writeTriplePatternFragment(outputStream, datasource, (ITriplePatternFragment) fragment, ldfRequest);
        } else if(fragment instanceof IStarPatternFragment){
            writeStarPatternFragment(outputStream, datasource, (IStarPatternFragment) fragment, ldfRequest);
        }
    }

    private void writeTriplePatternFragment(OutputStream outputStream, IDataSource datasource, ITriplePatternFragment fragment, ILinkedDataFragmentRequest ldfRequest) {
        final Model output = ModelFactory.createDefaultModel();
       // output.setNsPrefixes(getPrefixes());
        output.add(fragment.getMetadata());
        output.add(fragment.getTriples());
        output.add(fragment.getControls());
        RDFDataMgr.write(outputStream, output, contentType);
    }

    private void writeStarPatternFragment(OutputStream outputStream, IDataSource datasource, IStarPatternFragment fragment, ILinkedDataFragmentRequest ldfRequest) {
        final Model output = ModelFactory.createDefaultModel();
      //  output.setNsPrefixes(getPrefixes());
        output.add(fragment.getMetadata());
        output.add(fragment.getControls());
        RDFDataMgr.write(outputStream, output, contentType);

        for(Model out : fragment.getModels()) {
            RDFDataMgr.write(outputStream, out, contentType);
        }
    }
}
