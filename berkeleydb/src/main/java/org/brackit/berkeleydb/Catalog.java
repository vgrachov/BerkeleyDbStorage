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
import org.brackit.berkeleydb.binding.CatalogTupleBinding;
import org.brackit.berkeleydb.binding.IndexValueCreator;
import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.impl.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;

/**
 * Class for managing database catalog.
 * @author Volodymyr Grachov
 *
 */
public final class Catalog {

	private Database catalogDB = BerkeleyDBEnvironment.getInstance().getCatalog();
	private Environment environment = BerkeleyDBEnvironment.getInstance().getEnv();
	
	
	private static final Logger logger = Logger.getLogger(Catalog.class);
	
	private static class CatalogDBHolder{
		private static Catalog instance = new Catalog(); 
	}
	
	public static Catalog getInstance(){
		return CatalogDBHolder.instance;
	}

	private boolean addDatabaseToCatalog(Schema schema){
		//TODO: add check whether db is created
		DatabaseEntry keyEntry = new DatabaseEntry(schema.getDatabaseName().getBytes());
		DatabaseEntry valueEntry = new DatabaseEntry();
		
		CatalogTupleBinding catalogTupleBinding = new CatalogTupleBinding();
		catalogTupleBinding.objectToEntry(schema, valueEntry);
		catalogDB.put(null,keyEntry,valueEntry);
		return true;
	}
	
	private boolean createPrimaryDatabase(Schema schema){
		DatabaseConfig databaseConfig = new DatabaseConfig();
		databaseConfig.setTransactional(true);
		databaseConfig.setAllowCreate(true);
		Database db = environment.openDatabase(null, schema.getDatabaseName(), databaseConfig);
		db.close();
		return true;
	}

	
	private boolean createIndexes(Schema schema){
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
	/**
	 * Create Primary database with all indexes, represented in secondary databases
	 * @param schema - database schema
	 */
	public void createDatabase(Schema schema){
		logger.debug("Add schema");
		addDatabaseToCatalog(schema);
		logger.debug("Create main database"+schema.getDatabaseName());
		createPrimaryDatabase(schema);
		logger.debug("Create indexes");
		createIndexes(schema);
	}
	
	public Schema getSchemaByDatabaseName(String databaseName){
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
	
	public static void main(String[] args) {
		Catalog catalogDB = new Catalog();
		Schema schema = new Schema(new Column[]{
				new Column("CUSTOMER","C_CUSTKEY", ColumnType.Integer,true,true),
				new Column("CUSTOMER","C_NAME", ColumnType.String,false,true),
				new Column("CUSTOMER","C_ADDRESS", ColumnType.String,false,false),
				new Column("CUSTOMER","C_NATIONKEY", ColumnType.Integer,false,true),
				new Column("CUSTOMER","C_PHONE", ColumnType.String,false,false),
				new Column("CUSTOMER","C_ACCTBAL", ColumnType.Double,false,false),
				new Column("CUSTOMER","C_MKTSEGMENT", ColumnType.String,false,false),
				new Column("CUSTOMER","C_COMMENT", ColumnType.String,false,false)
		}, "CUSTOMER");
		catalogDB.createDatabase(schema);
		Schema schema2 = catalogDB.getSchemaByDatabaseName("CUSTOMER");
		for (int i=0;i<schema2.getColumns().length;i++){
			logger.debug("Print column "+schema2.getColumns()[i]);
		}
		BerkeleyDBEnvironment.getInstance().close();
		
	}
/*	CREATE TEXT TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL);
*/
}
