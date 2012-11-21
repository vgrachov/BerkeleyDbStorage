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
package org.brackit.berkeleydb.catalog;

import java.io.FileNotFoundException;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.Schema;
import org.brackit.berkeleydb.binding.CatalogTupleBinding;
import org.brackit.berkeleydb.binding.IndexValueCreator;
import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.exception.KeyDuplicationException;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.SecondaryConfig;
import com.sleepycat.db.SecondaryDatabase;
import com.sleepycat.db.Transaction;
import com.sleepycat.db.TransactionConfig;

/**
 * Class for managing database catalog. Add new table and delete.
 * @author Volodymyr Grachov
 *
 */
public final class Catalog implements ICatalog {

	private Database catalogDB = BerkeleyDBEnvironment.getInstance().getCatalog();
	private Environment environment = BerkeleyDBEnvironment.getInstance().getEnv();
	
	
	private static final Logger logger = Logger.getLogger(Catalog.class);
	
	private static class CatalogDBHolder{
		private static ICatalog instance = new Catalog(); 
	}
	
	private Catalog(){
		
	}
	
	public static ICatalog getInstance(){
		return CatalogDBHolder.instance;
	}

	private boolean addDatabaseToCatalog(Schema schema, Transaction transaction) throws KeyDuplicationException, DatabaseException{
		DatabaseEntry keyEntry = new DatabaseEntry(schema.getDatabaseName().getBytes());
		DatabaseEntry valueEntry = new DatabaseEntry();
		CatalogTupleBinding catalogTupleBinding = new CatalogTupleBinding();
		catalogTupleBinding.objectToEntry(schema, valueEntry);
		OperationStatus operationStatus = catalogDB.put(transaction,keyEntry,valueEntry);
		if (operationStatus==OperationStatus.KEYEXIST)
			throw new KeyDuplicationException("Database with name "+schema.getDatabaseName()+" is already exists");
		return true;
	}
	
	private boolean createPrimaryDatabase(Schema schema, Transaction transaction) throws FileNotFoundException, DatabaseException{
		DatabaseConfig databaseConfig = new DatabaseConfig();
		databaseConfig.setTransactional(true);
		databaseConfig.setAllowCreate(true);
		databaseConfig.setType(DatabaseType.BTREE);

		Database db = environment.openDatabase(null, schema.getDatabaseName(), null, databaseConfig);
		db.close();
		return true;
	}

	
	private boolean createIndexes(Schema schema, Transaction transaction) throws FileNotFoundException, DatabaseException{
		DatabaseConfig databaseConfig = new DatabaseConfig();
		databaseConfig.setTransactional(true);
		databaseConfig.setAllowCreate(true);
		databaseConfig.setType(DatabaseType.BTREE);
		Database primaryDatabase = environment.openDatabase(null, schema.getDatabaseName(), null, databaseConfig);
		RelationalTupleBinding relationalTupleBinding = new RelationalTupleBinding(schema.getColumns());
		SecondaryConfig secondaryConfig = BerkeleyDBEnvironment.getDefaultSecondDatabaseConfig(true);
		for (int i=0;i<schema.getColumns().length;i++)
			if (schema.getColumns()[i].isDirectIndexExist()){
				IndexValueCreator indexValueCreator = new IndexValueCreator(relationalTupleBinding, schema.getColumns()[i]);
				SecondaryConfig indexDatabaseConfiguration = (SecondaryConfig)secondaryConfig.cloneConfig();
				indexDatabaseConfiguration.setKeyCreator(indexValueCreator);
				String indexDatabaseName = schema.getDatabaseName()+"_"+schema.getColumns()[i].getColumnName()+"_index";
				logger.debug("Create :: "+ indexDatabaseName);
				SecondaryDatabase indexDatabase = environment.openSecondaryDatabase(null, indexDatabaseName, null, primaryDatabase, indexDatabaseConfiguration);
				indexDatabase.close();
			}
		primaryDatabase.close();
		return true;
	}
	
	/* (non-Javadoc)
	 * @see org.brackit.berkeleydb.ICatalog#createDatabase(org.brackit.berkeleydb.Schema)
	 */
	public void createDatabase(Schema schema) throws KeyDuplicationException{
		Transaction transaction = null;
		try{
			transaction = environment.beginTransaction(null, TransactionConfig.DEFAULT);
			logger.debug("Add schema");
			addDatabaseToCatalog(schema,transaction);
			logger.debug("Create main database"+schema.getDatabaseName());
			createPrimaryDatabase(schema,transaction);
			logger.debug("Create indexes");
			createIndexes(schema,transaction);
			transaction.commit();
		}catch (DatabaseException e) {
			if (transaction!=null)
				try {
					transaction.abort();
				} catch (DatabaseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		}catch (KeyDuplicationException e) {
			if (transaction!=null)
				try {
					transaction.abort();
				} catch (DatabaseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			throw e;
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		}
	}
	
	/* (non-Javadoc)
	 * @see org.brackit.berkeleydb.ICatalog#getSchemaByDatabaseName(java.lang.String)
	 */
	public Schema getSchemaByDatabaseName(String databaseName){
		if (databaseName==null || databaseName.isEmpty())
			throw new IllegalArgumentException("Database name can't be empty");
		logger.debug("Search for database "+databaseName);
		DatabaseEntry key = new DatabaseEntry(databaseName.getBytes());
		DatabaseEntry schemaEntry = new DatabaseEntry();
		OperationStatus status = null;
		try{
			status = catalogDB.get(null, key, schemaEntry, LockMode.READ_COMMITTED);
		}catch (DatabaseException e) {
			logger.error(e.getMessage());
			return null;
		}
		logger.debug("Search status "+status);
		if (status == OperationStatus.SUCCESS){
			CatalogTupleBinding catalogTupleBinding = new CatalogTupleBinding();
			TupleInput serializedSchema = new TupleInput(schemaEntry.getData());
			Schema schema = catalogTupleBinding.entryToObject(serializedSchema);
			return schema;
		}else
			return null;
	}
	
	private void deleteDatabaseFromCatalog(String databaseName, Transaction transaction){
		DatabaseEntry keyEntry = new DatabaseEntry(databaseName.getBytes());
		//TODO:review
		try {
			OperationStatus operationStatus = catalogDB.delete(transaction,keyEntry);
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//if (operationStatus == OperationStatus.NOTFOUND)
			//throw new DatabaseNotFoundException("Database "+databaseName+" is not found");
	}

	private void deletePrimaryDatabase(String databaseName, Transaction transaction){
		DatabaseEntry keyEntry = new DatabaseEntry(databaseName.getBytes());
		try {
			environment.removeDatabase(transaction, databaseName, databaseName);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/* (non-Javadoc)
	 * @see org.brackit.berkeleydb.ICatalog#deleteDatabase(java.lang.String)
	 */
	public boolean deleteDatabase(String databaseName){
		Transaction transaction = null;
		try{
			transaction = environment.beginTransaction(null, null);
			deleteDatabaseFromCatalog(databaseName,transaction);
			deletePrimaryDatabase(databaseName,transaction);
			transaction.commit();
			return true;
		}catch (DatabaseException e) {
			if (transaction!=null)
				try {
					transaction.abort();
				} catch (DatabaseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			return false;
		}
	}
	
}
