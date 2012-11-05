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
import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.Tuple;

import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class DatabaseAccess implements IDatabaseAccess {

	private static final Logger logger = Logger.getLogger(DatabaseAccess.class);
	
	protected final Database dataBase;
	protected final Schema schema;
	protected final Environment environment;
	protected RelationalTupleBinding tupleBinding;
	
	public DatabaseAccess(String databaseName){
		this.dataBase = BerkeleyDBEnvironment.getInstance().getDatabasereference(databaseName);
		this.schema = Catalog.getInstance().getSchemaByDatabaseName(databaseName);
		this.environment = BerkeleyDBEnvironment.getInstance().getEnv();
		this.tupleBinding = new RelationalTupleBinding(schema.getColumns());
	}
	
	public boolean insert(Tuple tuple){
		Transaction transaction = environment.beginTransaction(null, null);
		try{
			boolean insert = insert(tuple,transaction);
			transaction.commit();
			return insert;
		}catch (DatabaseException e) {
			if (transaction!=null)
				transaction.abort();
			return false;
		}
	}
	
	public boolean insert(Tuple tuple, Transaction transaction) {
		TupleOutput serializedTupleValues = new TupleOutput();
		TupleOutput serializedKey = new TupleOutput();
		tupleBinding.smartObjectToEntry(tuple, serializedKey, serializedTupleValues);
		dataBase.put(transaction, new DatabaseEntry(serializedKey.toByteArray()), new DatabaseEntry(serializedTupleValues.toByteArray()));
		return true;
	}
	
	

	public boolean update(DatabaseEntry key, DatabaseEntry value) {
		// TODO Auto-generated method stub
		return false;
	}
	
}
