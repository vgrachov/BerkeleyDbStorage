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
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.cursor.EqualMatchIndexSearchCursor;
import org.brackit.berkeleydb.cursor.ITupleCursor;
import org.brackit.berkeleydb.cursor.TupleCursor;
import org.brackit.berkeleydb.cursor.TupleCursor.CursorType;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Tuple;
import org.junit.AfterClass;
import org.junit.Test;

public class USER_DB_ITERATOR {

	private static final Logger logger = Logger.getLogger(USER_DB_ITERATOR.class);

	
	public void iterator(){
		ITupleCursor tupleCursor = new TupleCursor("USER_TABLE", CursorType.FullScan);
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
