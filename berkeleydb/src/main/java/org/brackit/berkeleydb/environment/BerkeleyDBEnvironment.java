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

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.Schema;
import org.brackit.berkeleydb.binding.IndexValueCreator;
import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.tuple.Column;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;

/**
 * Initialization of Berkeley DB environment. Provide interface for open berkeley database(table from relational view) and close them. 
 * Database instance is thread safe.
 * @author Volodymyr Grachov
 *
 */
public final class BerkeleyDBEnvironment implements IBerkeleyDBEnvironment {

	private static final Logger logger = Logger.getLogger(BerkeleyDBEnvironment.class);

    private static final String CATALOG = "catalog";
	
	
	private static class BerkeleyDBEnvironmentHolder{
		private static IBerkeleyDBEnvironment instance = new BerkeleyDBEnvironment();
	}

    private Environment env;
    private Database catalog;
    private Map<String,Database> databaseMap;
    private Map<Column,SecondaryDatabase> indexMap;
	
	private BerkeleyDBEnvironment(){
		logger.debug("Init database environment");
		
		EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        
        // Set the Berkeley DB Catalog config.
        DatabaseConfig catalogDBConfig = new DatabaseConfig();
        catalogDBConfig.setTransactional(true);
        catalogDBConfig.setAllowCreate(true);
        catalogDBConfig.setSortedDuplicates(false);
        
        
        
        env = new Environment(new File("c:/db"), envConfig);
        
        catalog = env.openDatabase(null, CATALOG, catalogDBConfig);
        
        databaseMap = new HashMap<String, Database>();
	}
	
	/* (non-Javadoc)
	 * @see org.brackit.berkeleydb.IBerkeleyDBEnvironment#getDatabasereference(java.lang.String)
	 */
	public synchronized Database getDatabasereference(String databaseName){
		logger.debug("Look for database "+databaseName);
		if (databaseMap.containsKey(databaseName)){
			logger.debug("Database is found in cache");
			return databaseMap.get(databaseName);
		}else{
			logger.debug("Open database "+databaseName);
			DatabaseConfig databaseConfig = new DatabaseConfig();
			databaseConfig.setTransactional(true);
			databaseConfig.setAllowCreate(false);
			Database db = env.openDatabase(null, databaseName, databaseConfig);
			Schema schema = Catalog.getInstance().getSchemaByDatabaseName(databaseName);
			RelationalTupleBinding relationalTupleBinding = new RelationalTupleBinding(schema.getColumns());
			
			for (int i=0;i<schema.getColumns().length;i++){
				if (schema.getColumns()[i].isDirectIndexExist()){
					SecondaryConfig secondaryConfig = getDefaultSecondDatabaseConfig(true).clone();
					IndexValueCreator indexValueCreator = new IndexValueCreator(relationalTupleBinding, schema.getColumns()[i]);
					String indexDatabaseName = db.getDatabaseName()+"_"+schema.getColumns()[i].getColumnName()+"_index";
					secondaryConfig.setKeyCreator(indexValueCreator);
					SecondaryDatabase secondaryDatabase = env.openSecondaryDatabase(null, indexDatabaseName, db, secondaryConfig);
					databaseMap.put(indexDatabaseName, secondaryDatabase);
				}
			}
			databaseMap.put(databaseName, db);
			return db;
		}
	}
	
	public static SecondaryConfig getDefaultSecondDatabaseConfig(boolean allowCreate){
		SecondaryConfig secondaryConfig = new SecondaryConfig();
		secondaryConfig.setTransactional(true);
		secondaryConfig.setAllowCreate(allowCreate);
		secondaryConfig.setAllowPopulate(!allowCreate);
		secondaryConfig.setSortedDuplicates(true);
		return secondaryConfig; 
	}
	
	public synchronized SecondaryDatabase getIndexreference(Column index){
		String primaryDatabaseName = index.getDatabaseName();
		String indexDatabaseName = index.getDatabaseName()+"_"+index.getColumnName()+"_index";
		logger.debug("Look for database "+indexDatabaseName);
		if (databaseMap.containsKey(indexDatabaseName)){
			logger.debug("Database is found in cache");
			return (SecondaryDatabase)databaseMap.get(indexDatabaseName);
		}else{
			logger.debug("Open database "+indexDatabaseName);
			Database primaryDatabase = getDatabasereference(primaryDatabaseName);
			/*Schema schema = Catalog.getInstance().getSchemaByDatabaseName(primaryDatabaseName);
			RelationalTupleBinding relationalTupleBinding = new RelationalTupleBinding(schema.getColumns());
			IndexValueCreator indexValueCreator = new IndexValueCreator(relationalTupleBinding, index);
			SecondaryConfig secondaryConfig = getDefaultSecondDatabaseConfig(false).clone();
			secondaryConfig.setKeyCreator(indexValueCreator);

			SecondaryDatabase secondaryDatabase = env.openSecondaryDatabase(null, indexDatabaseName, primaryDatabase, secondaryConfig);*/
			return (SecondaryDatabase)databaseMap.get(indexDatabaseName);
		}
	}
	
	public static IBerkeleyDBEnvironment getInstance(){
		return BerkeleyDBEnvironmentHolder.instance; 
	}
    /* (non-Javadoc)
	 * @see org.brackit.berkeleydb.IBerkeleyDBEnvironment#getEnv()
	 */
    public Environment getEnv() {
		return env;
	}

	/* (non-Javadoc)
	 * @see org.brackit.berkeleydb.IBerkeleyDBEnvironment#getCatalog()
	 */
	public Database getCatalog() {
		return catalog;
	}

	/* (non-Javadoc)
	 * @see org.brackit.berkeleydb.IBerkeleyDBEnvironment#close()
	 */
    public void close() throws DatabaseException {
    	Iterator<Map.Entry<String, Database>> databaseIterator = databaseMap.entrySet().iterator();
    	while (databaseIterator.hasNext()){
    		Map.Entry<String, Database> entry = (Map.Entry<String, Database>)databaseIterator.next();
    		Database db = entry.getValue();
    		if (db instanceof SecondaryDatabase)
    			db.close();
    	}
    	databaseIterator = databaseMap.entrySet().iterator();
    	while (databaseIterator.hasNext()){
    		Map.Entry<String, Database> entry = (Map.Entry<String, Database>)databaseIterator.next();
    		Database db = entry.getValue();
    		if (!(db instanceof SecondaryDatabase))
    			db.close();
    	}
    	databaseMap.clear();
        catalog.close();
        env.close();
    }
	
}
