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
package org.brackit.berkeleydb;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.cursor.ITupleCursor;
import org.brackit.berkeleydb.cursor.RangeIndexSearchCursor;
import org.brackit.berkeleydb.impl.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Tuple;
import org.junit.AfterClass;
import org.junit.Test;

import com.sleepycat.je.utilint.IntegralRateStat;

public class USER_DB_RANGE_SEARCH {

	private static final Logger logger = Logger.getLogger(USER_DB_RANGE_SEARCH.class);

	@Test
	public void rangeSearch(){
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName("USER_TABLE");
		AtomicInteger leftKey = new AtomicInteger("Login", 9);
		AtomicInteger rightKey = new AtomicInteger("Login", 9);
		ITupleCursor cursor = new RangeIndexSearchCursor(schema.getColumns()[4], leftKey, rightKey);
		cursor.open();
		Tuple tuple = null;
		while ((tuple = cursor.next())!=null){
			logger.debug("Range result "+tuple);
		}
		cursor.close();
	}
	
	@AfterClass
	public static void close(){
		BerkeleyDBEnvironment.getInstance().close();
	}
	
}
