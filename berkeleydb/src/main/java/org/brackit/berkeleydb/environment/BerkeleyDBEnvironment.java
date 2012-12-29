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
package org.brackit.berkeleydb.environment;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.Schema;
import org.brackit.berkeleydb.binding.IndexValueCreator;
import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.tuple.Column;

import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.LockDetectMode;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.SecondaryConfig;
import com.sleepycat.db.SecondaryDatabase;

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

    private com.sleepycat.db.Environment env;
    private Database catalog;
    private Map<String,Database> databaseMap;
    private Map<Column,SecondaryDatabase> indexMap;
	
	private BerkeleyDBEnvironment(){
		logger.debug("Init database environment");
	
        EnvironmentConfig myEnvConfig = new EnvironmentConfig();

        // Region files are not backed by the filesystem, they are
        // backed by heap memory.
        myEnvConfig.setPrivate(true);
        myEnvConfig.setAllowCreate(true);
        myEnvConfig.setInitializeCache(true);
        myEnvConfig.setInitializeLocking(true);
        myEnvConfig.setInitializeLogging(true);
        myEnvConfig.setThreaded(true);


        myEnvConfig.setTransactional(true);
        // EnvironmentConfig.setThreaded(true) is the default behavior
        // in Java, so we do not have to do anything to cause the
        // environment handle to be free-threaded.

        // Indicate that we want db to internally perform deadlock
        // detection. Also indicate that the transaction that has
        // performed the least amount of write activity to
        // receive the deadlock notification, if any.
        myEnvConfig.setLockDetectMode(LockDetectMode.MINWRITE);

        // Specify in-memory logging
        //myEnvConfig.setLogInMemory(true);
        // Specify the size of the in-memory log buffer
        // Must be large enough to handle the log data created by
        // the largest transaction.
        //myEnvConfig.setLogBufferSize(10 * 1024 * 1024);
        // Specify the size of the in-memory cache
        // Set it large enough so that it won't page.
        myEnvConfig.setCacheSize(10 * 1024 * 1024);
        myEnvConfig.setLogAutoRemove(true);

        


        // Set the Berkeley DB Catalog config.
        DatabaseConfig catalogDBConfig = new DatabaseConfig();
        catalogDBConfig.setTransactional(true);
        catalogDBConfig.setAllowCreate(true);
        catalogDBConfig.setSortedDuplicates(false);
        catalogDBConfig.setType(DatabaseType.BTREE);

        
        try{
        	env = new Environment(new File("c:/db10mb/"), myEnvConfig);
        	
        	catalog = env.openDatabase(null,CATALOG, null, catalogDBConfig);
        } catch (DatabaseException e) {
			logger.error(e.getMessage());
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		}
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
			if (databaseName.equals("orders"))
				databaseConfig.setType(DatabaseType.BTREE);
			else
				databaseConfig.setType(DatabaseType.BTREE);
			databaseConfig.setCacheSize(100000000);
			
			
			Database db = null;
			try {
				db = env.openDatabase(null,databaseName, null, databaseConfig);
				
			} catch (FileNotFoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			} catch (DatabaseException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			Schema schema = Catalog.getInstance().getSchemaByDatabaseName(databaseName);
			RelationalTupleBinding relationalTupleBinding = new RelationalTupleBinding(schema.getColumns());
			
			for (int i=0;i<schema.getColumns().length;i++){
				if (schema.getColumns()[i].isDirectIndexExist()){
					SecondaryConfig secondaryConfig = (SecondaryConfig)getDefaultSecondDatabaseConfig(true).cloneConfig();
					IndexValueCreator indexValueCreator = new IndexValueCreator(relationalTupleBinding, schema.getColumns()[i]);
					String indexDatabaseName = null;
					indexDatabaseName = databaseName+"_"+schema.getColumns()[i].getColumnName()+"_index";
					secondaryConfig.setKeyCreator(indexValueCreator);
					secondaryConfig.setCacheSize(100000000);
					
					SecondaryDatabase secondaryDatabase = null;
					try {
						secondaryDatabase = env.openSecondaryDatabase(null,indexDatabaseName, null, db, secondaryConfig);
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (DatabaseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
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
		secondaryConfig.setType(DatabaseType.BTREE);
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
    public void close() {
    	Iterator<Map.Entry<String, Database>> databaseIterator = databaseMap.entrySet().iterator();
    	while (databaseIterator.hasNext()){
    		Map.Entry<String, Database> entry = (Map.Entry<String, Database>)databaseIterator.next();
    		Database db = entry.getValue();
    		if (db instanceof SecondaryDatabase)
				try {
					db.close();
				} catch (DatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    	}
    	databaseIterator = databaseMap.entrySet().iterator();
    	while (databaseIterator.hasNext()){
    		Map.Entry<String, Database> entry = (Map.Entry<String, Database>)databaseIterator.next();
    		Database db = entry.getValue();
    		if (!(db instanceof SecondaryDatabase))
				try {
					db.close();
				} catch (DatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    	}
    	databaseMap.clear();
        try {
			catalog.close();
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			env.close();
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	
}
