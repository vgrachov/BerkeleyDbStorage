/*******************************************************************************
 * Copyright 2012 Volodymyr Grachov
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
		while ((tuple = cursor.next())!=null){
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
		AtomicString leftRange = new AtomicString("l_shipdate", "1998-10-09");
		ITupleCursor cursor = new RangeIndexSearchCursor(schema.getColumns()[10], null, null);
		cursor.open();
		Tuple tuple = null;
		int i=0;
		while ((tuple = cursor.next())!=null){
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
