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

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.cursor.ITupleCursor;
import org.brackit.berkeleydb.cursor.TupleCursor;
import org.brackit.berkeleydb.cursor.TupleCursor.CursorType;
import org.brackit.berkeleydb.impl.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CUSTOMER_DB_Iterator {

	private static final Logger logger = Logger.getLogger(CUSTOMER_DB_Iterator.class);
	
	private static ITupleCursor cursor;
	
	@BeforeClass
	public static void init(){
		cursor = new TupleCursor("CUSTOMER",CursorType.FullScan);
	}
	
	@Test
	public void alltuples() {
		cursor.open();
		Tuple tuple = null;
		int count = 0;
		while ((tuple = cursor.next())!=null){
			logger.debug("Iterator "+count+" - "+tuple);
			count++;
		}
		Assert.assertEquals(count, 18);
	}
	
	@AfterClass
	public static void close(){
		cursor.close();
		BerkeleyDBEnvironment.getInstance().close();
	}

}
