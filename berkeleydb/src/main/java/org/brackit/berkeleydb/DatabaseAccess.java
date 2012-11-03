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

import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.impl.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.Tuple;

import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class DatabaseAccess implements IDatabaseAccess {

	protected final String databaseName;
	protected final Database dataBase;
	protected final Schema schema;
	
	public DatabaseAccess(String databaseName){
		this.databaseName = databaseName;
		this.dataBase = BerkeleyDBEnvironment.getInstance().getDatabasereference(databaseName);
		this.schema = Catalog.getInstance().getSchemaByDatabaseName(databaseName);
	}
	
	public boolean insert(Tuple tuple) {
		Environment environment = BerkeleyDBEnvironment.getInstance().getEnv();
		Transaction txn = environment.beginTransaction(null, null);
		TupleOutput serializedTupleValues = new TupleOutput();
		TupleOutput serializedKey = new TupleOutput();
		RelationalTupleBinding tupleBinding = new RelationalTupleBinding(schema.getColumns());
		tupleBinding.smartObjectToEntry(tuple, serializedKey, serializedTupleValues);
		try{
			dataBase.put(txn, new DatabaseEntry(serializedKey.getBufferBytes()), new DatabaseEntry(serializedTupleValues.getBufferBytes()));
			txn.commit();
		} catch (Exception e) {
			
			if (txn!=null)
				txn.abort();
		}
		return false;
	}
	
	

	public boolean update(DatabaseEntry key, DatabaseEntry value) {
		// TODO Auto-generated method stub
		return false;
	}
	
}
