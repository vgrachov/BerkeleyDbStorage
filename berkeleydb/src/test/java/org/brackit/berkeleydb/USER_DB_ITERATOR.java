/*******************************************************************************
 * [New BSD License]
 *  Copyright (c) 2012, Volodymyr Grachov <vladimir.grachov@gmail.com>  
 *  All rights reserved.
 *  
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *      * Redistributions of source code must retain the above copyright
 *        notice, this list of conditions and the following disclaimer.
 *      * Redistributions in binary form must reproduce the above copyright
 *        notice, this list of conditions and the following disclaimer in the
 *        documentation and/or other materials provided with the distribution.
 *      * Neither the name of the Brackit Project Team nor the
 *        names of its contributors may be used to endorse or promote products
 *        derived from this software without specific prior written permission.
 *  
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 *  ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package org.brackit.berkeleydb;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.cursor.EqualMatchIndexSearchCursor;
import org.brackit.berkeleydb.cursor.ITupleCursor;
import org.brackit.berkeleydb.cursor.FullTableScanCursor;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Tuple;
import org.junit.AfterClass;
import org.junit.Test;

public class USER_DB_ITERATOR {

	private static final Logger logger = Logger.getLogger(USER_DB_ITERATOR.class);

	
	public void iterator(){
		ITupleCursor tupleCursor = new FullTableScanCursor("USER_TABLE");
		tupleCursor.open();
		Tuple tuple = null;
		while ((tuple = tupleCursor.next())!=null){
			//logger.debug("Iterator "+tuple);
		}
		tupleCursor.close();
		//BerkeleyDBEnvironment.getInstance().close();
	}
	@Test
	public void testEqualMatchIndexSearchCursor(){
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName("USER_TABLE");
		ITupleCursor tupleCursor = new EqualMatchIndexSearchCursor("USER_TABLE",schema.getColumns()[4], new AtomicInteger("Test", 0));
		tupleCursor.open();
		Tuple tuple = null;
		
		while ((tuple = tupleCursor.next())!=null){
			logger.debug("Equal match test "+tuple);
		}
		tupleCursor.close();
	}
	
	@AfterClass
	public static void close(){
		BerkeleyDBEnvironment.getInstance().close();
	}
}
