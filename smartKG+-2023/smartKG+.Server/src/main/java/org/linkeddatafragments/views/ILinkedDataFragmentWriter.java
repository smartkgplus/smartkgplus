package org.linkeddatafragments.views;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;

import java.io.OutputStream;


public interface ILinkedDataFragmentWriter {
    /**
     * Writes a 404 Not Found error
     *
     * @param outputStream The response stream to write to
     * @param request Request that is unable to answer
     * @throws Exception Error that occurs while serializing
     */
    public void writeNotFound(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    /**
     * Writes a 5XX error
     *
     * @param outputStream The response stream to write to
     * @param ex Exception that occurred
     * @throws Exception Error that occurs while serializing
     */
    public void writeError(ServletOutputStream outputStream, Exception ex) throws Exception;

    /**
     * Serializes and writes a {@link ILinkedDataFragment}
     *
     * @param outputStream The response stream to write to
     * @param datasource
     * @param fragment
     * @param ldfRequest Parsed request for fragment
     * @throws Exception Error that occurs while serializing
     */
    public void writeFragment(OutputStream outputStream, IDataSource datasource, ILinkedDataFragment fragment, ILinkedDataFragmentRequest ldfRequest) throws Exception;
}
