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
		logger.info("Scan time : "+(System.currentTimeMillis()-start));
		Assert.assertEquals(counter, 1500);
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
