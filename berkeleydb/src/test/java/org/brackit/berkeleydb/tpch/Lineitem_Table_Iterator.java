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
import org.brackit.berkeleydb.Schema;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.cursor.FullIndexIterator;
import org.brackit.berkeleydb.cursor.ITupleCursor;
import org.brackit.berkeleydb.cursor.RangeIndexSearchCursor;
import org.brackit.berkeleydb.cursor.FullTableScanCursor;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.AtomicDouble;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Tuple;
import org.junit.AfterClass;
import org.junit.Test;

import com.sleepycat.db.DatabaseException;

public class Lineitem_Table_Iterator {

	private static final Logger logger = Logger.getLogger(Lineitem_Table_Iterator.class);
	private static String tableName = "lineitem"; 
	

	@Test
	public void fullIterator(){
		logger.debug("Full scan");
		ITupleCursor cursor = new FullTableScanCursor(tableName);
		long start = System.currentTimeMillis();
		cursor.open();
		Tuple tuple = null;
		int i=0;
		while ((tuple = cursor.next())!=null){
			if (i%10000==0)
				logger.debug(i);
			i++;
			//logger.debug(tuple);
		}
		cursor.close();
		logger.info("Full scan time "+(System.currentTimeMillis()-start)+" "+i);
	}

	
	public void fullIterator1(){
		logger.debug("Full scan");
		ITupleCursor cursor = new FullTableScanCursor(tableName);
		long start = System.currentTimeMillis();
		cursor.open();
		Tuple tuple = null;
		int i=0;
		while ((tuple = cursor.next())!=null){
			if (i%10000==0)
				logger.debug(i);
			i++;
			//logger.debug(tuple);
		}
		cursor.close();
		logger.info("Full scan time "+(System.currentTimeMillis()-start));
	}
	
	
	public void fullIndexIterator(){
		logger.debug("Full index scan");
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(tableName);
		long start = System.currentTimeMillis();
		ITupleCursor cursor = new FullIndexIterator(schema.getColumns()[10]);
		cursor.open();
		Tuple tuple = null;
		while ((tuple = cursor.next())!=null){
			//logger.debug(tuple.getFields()[10]);
		}
		cursor.close();
		logger.info("Full scan time "+(System.currentTimeMillis()-start));
	}

	public void rangeSearch(){
		logger.debug("Range index scan");
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(tableName);
		AtomicDouble leftRange = new AtomicDouble("l_discount", 0.02);
		AtomicDouble rightRange = new AtomicDouble("l_discount", 0.03);
		ITupleCursor cursor = new RangeIndexSearchCursor(schema.getColumns()[6], leftRange, rightRange);
		cursor.open();
		Tuple tuple = null;
		while ((tuple = cursor.next())!=null){
			logger.debug(tuple.getFields()[6]);
		}
		cursor.close();
	}


	
	public void rangeIteratorShipdate(){
		logger.debug("Range index scan");
		long start = System.currentTimeMillis();
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(tableName);
		AtomicString rightRange = new AtomicString("l_shipdate", "1994-06-27");
		AtomicString leftRange = new AtomicString("l_shipdate", "1992-06-27");
		ITupleCursor cursor = new RangeIndexSearchCursor(schema.getColumns()[10], leftRange, rightRange);
		cursor.open();
		Tuple tuple = null;
		int i=0;
		while ((tuple = cursor.next())!=null){
			if (i%10000==0)
				logger.debug(i);

			//logger.debug(tuple.getFields()[10]);
			i++;
		}
		cursor.close();
		logger.info("Full scan time "+(System.currentTimeMillis()-start)+" found "+i+" records");
	}
	
	public void fullIteratorDateIndex(){
		logger.debug("Full index scan");
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(tableName);
		ITupleCursor cursor = new FullIndexIterator(schema.getColumns()[10]);
		cursor.open();
		Tuple tuple = null;
		while ((tuple = cursor.next())!=null){
			logger.debug(tuple.getFields()[10]);
		}
		cursor.close();
	}
	
	@AfterClass
	public static void close(){
		BerkeleyDBEnvironment.getInstance().close();
	}
	
}
