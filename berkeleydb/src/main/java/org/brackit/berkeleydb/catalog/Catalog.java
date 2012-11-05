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
package org.brackit.berkeleydb.catalog;

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
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;

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

	private boolean addDatabaseToCatalog(Schema schema, Transaction transaction) throws KeyDuplicationException{
		DatabaseEntry keyEntry = new DatabaseEntry(schema.getDatabaseName().getBytes());
		DatabaseEntry valueEntry = new DatabaseEntry();
		CatalogTupleBinding catalogTupleBinding = new CatalogTupleBinding();
		catalogTupleBinding.objectToEntry(schema, valueEntry);
		OperationStatus operationStatus = catalogDB.putNoOverwrite(transaction,keyEntry,valueEntry);
		if (operationStatus==OperationStatus.KEYEXIST)
			throw new KeyDuplicationException("Database with name "+schema.getDatabaseName()+" is already exists");
		return true;
	}
	
	private boolean createPrimaryDatabase(Schema schema, Transaction transaction){
		DatabaseConfig databaseConfig = new DatabaseConfig();
		databaseConfig.setTransactional(true);
		databaseConfig.setAllowCreate(true);
		Database db = environment.openDatabase(null, schema.getDatabaseName(), databaseConfig);
		db.close();
		return true;
	}

	
	private boolean createIndexes(Schema schema, Transaction transaction){
		DatabaseConfig databaseConfig = new DatabaseConfig();
		databaseConfig.setTransactional(true);
		databaseConfig.setAllowCreate(true);
		Database primaryDatabase = environment.openDatabase(null, schema.getDatabaseName(), databaseConfig);
		RelationalTupleBinding relationalTupleBinding = new RelationalTupleBinding(schema.getColumns());
		SecondaryConfig secondaryConfig = BerkeleyDBEnvironment.getDefaultSecondDatabaseConfig(true);
		for (int i=0;i<schema.getColumns().length;i++)
			if (schema.getColumns()[i].isDirectIndexExist()){
				IndexValueCreator indexValueCreator = new IndexValueCreator(relationalTupleBinding, schema.getColumns()[i]);
				SecondaryConfig indexDatabaseConfiguration = secondaryConfig.clone();
				indexDatabaseConfiguration.setKeyCreator(indexValueCreator);
				String indexDatabaseName = schema.getDatabaseName()+"_"+schema.getColumns()[i].getColumnName()+"_index";
				logger.debug("Create :: "+ indexDatabaseName);
				SecondaryDatabase indexDatabase = environment.openSecondaryDatabase(null, indexDatabaseName, primaryDatabase, indexDatabaseConfiguration);
				indexDatabase.close();
			}
		primaryDatabase.close();
		return true;
	}
	
	/* (non-Javadoc)
	 * @see org.brackit.berkeleydb.ICatalog#createDatabase(org.brackit.berkeleydb.Schema)
	 */
	public void createDatabase(Schema schema) throws KeyDuplicationException{
		Transaction transaction = environment.beginTransaction(null, null);
		try{
			logger.debug("Add schema");
			addDatabaseToCatalog(schema,transaction);
			logger.debug("Create main database"+schema.getDatabaseName());
			createPrimaryDatabase(schema,transaction);
			logger.debug("Create indexes");
			createIndexes(schema,transaction);
			transaction.commit();
		}catch (DatabaseException e) {
			if (transaction!=null)
				transaction.abort();
		}catch (KeyDuplicationException e) {
			if (transaction!=null)
				transaction.abort();
			throw e;
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
		OperationStatus status = catalogDB.get(null, key, schemaEntry, LockMode.READ_COMMITTED);
		logger.debug("Search status "+status);
		if (status == OperationStatus.SUCCESS){
			CatalogTupleBinding catalogTupleBinding = new CatalogTupleBinding();
			TupleInput serializedSchema = new TupleInput(schemaEntry.getData());
			Schema schema = catalogTupleBinding.entryToObject(serializedSchema);
			return schema;
		}else
			return null;
	}
	
	private void deleteDatabaseFromCatalog(String databaseName, Transaction transaction)throws DatabaseNotFoundException{
		DatabaseEntry keyEntry = new DatabaseEntry(databaseName.getBytes());
		OperationStatus operationStatus = catalogDB.delete(transaction,keyEntry);
		if (operationStatus == OperationStatus.NOTFOUND)
			throw new DatabaseNotFoundException("Database "+databaseName+" is not found");
	}

	private void deletePrimaryDatabase(String databaseName, Transaction transaction) throws DatabaseNotFoundException{
		DatabaseEntry keyEntry = new DatabaseEntry(databaseName.getBytes());
		environment.removeDatabase(transaction, databaseName);
	}
	
	
	/* (non-Javadoc)
	 * @see org.brackit.berkeleydb.ICatalog#deleteDatabase(java.lang.String)
	 */
	public boolean deleteDatabase(String databaseName){
		Transaction transaction = environment.beginTransaction(null, null);
		try{
			deleteDatabaseFromCatalog(databaseName,transaction);
			deletePrimaryDatabase(databaseName,transaction);
			transaction.commit();
			return true;
		}catch (DatabaseException e) {
			if (transaction!=null)
				transaction.abort();
			return false;
		}
	}
	
}
