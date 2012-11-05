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
package org.brackit.berkeleydb.environment;

import org.brackit.berkeleydb.tuple.Column;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.SecondaryDatabase;

public interface IBerkeleyDBEnvironment {

	/**
	 * Get Reference to database ( table  for RDBMS ). If database is not exist, it will be created
	 * @param databaseName
	 * @return
	 */
	public Database getDatabasereference(String databaseName);

	/**
	 * Get reference to index database. If index not exist, it will crate it.
	 * @param index
	 * @return
	 */
	public SecondaryDatabase getIndexreference(Column index);
	
	public Environment getEnv();

	/**
	 * Reference to Catalog database.
	 * @return
	 */
	public Database getCatalog();

	/**
	 * Close all databases and the environment.
	 */
	public void close() throws DatabaseException;
	
}
