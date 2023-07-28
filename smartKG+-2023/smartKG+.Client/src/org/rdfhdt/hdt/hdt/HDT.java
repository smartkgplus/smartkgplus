/*
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/hdt/HDT.java $
 * Revision: $Rev: 191 $
 * Last modified: $Date: 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $
 * Last modified by: $Author: mario.arias $
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * Contacting the authors:
 *   Mario Arias:               mario.arias@deri.org
 *   Javier D. Fernandez:       jfergar@infor.uva.es
 *   Miguel A. Martinez-Prieto: migumar2@infor.uva.es
 *   Alejandro Andres:          fuzzy.alej@gmail.com
 */

package org.rdfhdt.hdt.hdt;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.wisekg.model.BindingHashMap;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.header.Header;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.rdf.RDFAccess;
import org.rdfhdt.hdt.triples.IteratorStarString;
import org.rdfhdt.hdt.triples.IteratorsTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.triples.Triples;
import org.rdfhdt.hdt.util.StarString;


/**
 * Interface that specifies the methods for a HDT implementation
 *
 * @author mario.arias
 *
 */
public interface HDT extends RDFAccess,Closeable {

	/**
	 * Gets the header of the HDT
	 *
	 * @return Header
	 */
	Header getHeader();

	/**
	 * Gets the dictionary of the HDT
	 *
	 * @return Dictionary
	 */
	Dictionary getDictionary();

	/**
	 * Gets the triples of the HDT
	 *
	 * @return Triples
	 */
	Triples getTriples();

	/**
	 * Saves to OutputStream in HDT format
	 *
	 * @param output
	 *            The OutputStream to save to
	 */
	void saveToHDT(OutputStream output, ProgressListener listener) throws IOException;

	/**
	 * Saves to a file in HDT format
	 *
	 * @param output
	 *            The OutputStream to save to
	 */
	void saveToHDT(String fileName, ProgressListener listener) throws IOException;

	/**
	 * Returns the size of the Data Structure in bytes.
	 * @return
	 */
	long size();

	/**
	 * Get the Base URI for the Dataset.
	 * @return
	 */
	String getBaseURI();

	IteratorStarString searchStar(StarString star);

	IteratorStarString searchStarBindings(StarString star, List<BindingHashMap> bindings);

	IteratorsTripleString searchStar(ArrayList<TripleString> patterns);
}
