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
package org.brackit.berkeleydb.tpch;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.catalog.ICatalog;
import org.brackit.berkeleydb.cursor.EqualMatchIndexSearchCursor;
import org.brackit.berkeleydb.cursor.FullTableScanCursor;
import org.brackit.berkeleydb.cursor.ITupleCursor;
import org.brackit.berkeleydb.cursor.RangeIndexSearchCursor;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.Tuple;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class Customer_Table_Iterator {

	private static final Logger logger = Logger.getLogger(Customer_Table_Iterator.class);
	private static String tableName = "customer";
	private static ICatalog catalog;

	@BeforeClass
	public static void init(){
		catalog = Catalog.getInstance();
	}
	
	@Test
	public void fullIterator(){
		ITupleCursor cursor = new FullTableScanCursor(tableName);
		long start = System.currentTimeMillis();
		cursor.open();
		Tuple tuple = null;
		int counter = 0;
		while ((tuple=cursor.next())!=null){
			counter++;
		}
		cursor.close();
		logger.info("Rows count : "+counter);
		logger.info("Scan time : "+(System.currentTimeMillis()-start));
	}
	
	@Test
	public void indexScan(){
		//c_mktsegment
		Column column = catalog.getSchemaByDatabaseName(tableName).getColumns()[6];
		long start = System.currentTimeMillis();
		AtomicString value = new AtomicString("c_mktsegment", "BUILDING");
		ITupleCursor cursor = new EqualMatchIndexSearchCursor(tableName, column, value);
		cursor.open();
		Tuple tuple = null;
		int counter = 0;
		while ((tuple = cursor.next())!=null){
			logger.debug(tuple);
			counter++;
		}
		cursor.close();
		logger.info("Equal match found : "+counter);
		logger.info("Equal match search : "+(System.currentTimeMillis()-start));
	}
	

	@AfterClass
	public static void close(){
		BerkeleyDBEnvironment.getInstance().close();
	}
	
}
