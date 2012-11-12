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

import java.util.concurrent.atomic.AtomicBoolean;

import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.Atomic;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Tuple;
import org.junit.Test;

import com.sleepycat.je.Environment;

public class USER_DB_INSERT {

	@Test
	public void fillDatabase(){
		IDatabaseAccess databaseAccess = new DatabaseAccess("USER_TABLE");
		for (int i=0;i<5;i++){
			Atomic[] fields = new Atomic[6];
			fields[0] = new AtomicInteger("ID", i);
			fields[1] = new AtomicString("Login", "user"+i);
			fields[2] = new AtomicString("Password", "password"+i%3);
			fields[3] = new AtomicInteger("Salary", 100-i);
			fields[4] = new AtomicInteger("Test", i%10);
			fields[5] = new AtomicString("Birthday", "test");
			Tuple tuple = new Tuple(fields);
			databaseAccess.insert(tuple);
		}
		BerkeleyDBEnvironment.getInstance().close();
	}
	
}
