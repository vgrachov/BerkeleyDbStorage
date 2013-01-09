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
package org.brackit.berkeleydb;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.Tuple;

import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Environment;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.Transaction;
import com.sleepycat.db.TransactionConfig;

public class DatabaseAccess implements IDatabaseAccess {

	private static final Logger logger = Logger.getLogger(DatabaseAccess.class);
	
	protected final Database dataBase;
	protected final Schema schema;
	protected final Environment environment;
	protected final RelationalTupleBinding tupleBinding;
	
	public DatabaseAccess(String databaseName){
		this.dataBase = BerkeleyDBEnvironment.getInstance().getDatabasereference(databaseName);
		this.schema = Catalog.getInstance().getSchemaByDatabaseName(databaseName);
		this.environment = BerkeleyDBEnvironment.getInstance().getEnv();
		this.tupleBinding = new RelationalTupleBinding(schema.getColumns());
	}
	
	public boolean insert(Tuple tuple){
		Transaction transaction = null;
		try{
			transaction = environment.beginTransaction(null, TransactionConfig.DEFAULT);
			boolean insert = insert(tuple,transaction);
			transaction.commit();
			return insert;
		}catch (DatabaseException e) {
			if (transaction!=null)
				try {
					transaction.abort();
				} catch (DatabaseException e1) {
					logger.error(e1.getMessage());
				}
			return false;
		}
	}
	
	public boolean insert(Tuple tuple, Transaction transaction) {
		TupleOutput serializedTupleValues = new TupleOutput();
		TupleOutput serializedKey = new TupleOutput();
		tupleBinding.smartObjectToEntry(tuple, serializedKey, serializedTupleValues);
		try {
			//TODO:review
			OperationStatus status = dataBase.put(transaction, new DatabaseEntry(serializedKey.toByteArray()), new DatabaseEntry(serializedTupleValues.toByteArray()));
			if (status != OperationStatus.SUCCESS)
				logger.debug(status);
		} catch (DatabaseException e) {
			logger.error(e.getMessage());
		}
		return true;
	}
	
	

	public boolean update(DatabaseEntry key, DatabaseEntry value) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean delete(Tuple tuple){
		Transaction transaction = null;
		try{
			transaction = environment.beginTransaction(null, TransactionConfig.DEFAULT);
			boolean delete = delete(tuple,transaction);
			transaction.commit();
			return delete;
		}catch (DatabaseException e) {
			if (transaction!=null)
				try {
					transaction.abort();
				} catch (DatabaseException e1) {
					logger.error(e1.getMessage());
				}
			return false;
		}
	}
	
	public boolean delete(Tuple tuple, Transaction transaction){
		TupleOutput serializedTupleValue = new TupleOutput();
		TupleOutput serializedKey = new TupleOutput();
		tupleBinding.smartObjectToEntry(tuple, serializedKey, serializedTupleValue);
		try {
			OperationStatus status = dataBase.delete(transaction, new DatabaseEntry(serializedKey.toByteArray()));
			if (status == OperationStatus.SUCCESS)
				return true;
			else
				return false;
		} catch (DatabaseException e) {
			logger.error(e.getMessage());
		}
		return false;
	}

	
}
