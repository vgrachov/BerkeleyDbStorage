/*******************************************************************************
 * [New BSD License]
 *   Copyright (c) 2012-2013, Volodymyr Grachov <vladimir.grachov@gmail.com>  
 *   All rights reserved.
 *   
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions are met:
 *       * Redistributions of source code must retain the above copyright
 *         notice, this list of conditions and the following disclaimer.
 *       * Redistributions in binary form must reproduce the above copyright
 *         notice, this list of conditions and the following disclaimer in the
 *         documentation and/or other materials provided with the distribution.
 *       * Neither the name of the Brackit Project Team nor the
 *         names of its contributors may be used to endorse or promote products
 *         derived from this software without specific prior written permission.
 *   
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *   ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *   WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *   DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 *   ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *   (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *   LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package org.brackit.berkeleydb.transaction;

import java.io.File;
import java.io.FileNotFoundException;

import javax.sql.DataSource;

import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.db.CheckpointConfig;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.CursorConfig;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.LockDetectMode;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.MultipleDataEntry;
import com.sleepycat.db.MultipleEntry;
import com.sleepycat.db.MultipleKeyDataEntry;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.Transaction;
import com.sleepycat.db.TransactionConfig;
 
public class Transactiontest {

	private static final String path = "/home/vgrachov/Projects/test";
    private static final int buffLen = 100 * 1024 * 1024;
	
	/**
	 * @param args
	 * @throws DatabaseException 
	 * @throws FileNotFoundException 
	 */
	private static void createDatabase(Transaction transaction, Database database) throws DatabaseException{
        MultipleKeyDataEntry pairSet;
        pairSet = new MultipleKeyDataEntry();
        pairSet.setData(new byte[buffLen * 2]);
        pairSet.setUserBuffer(buffLen * 2, true);
        
		MultipleDataEntry multipleKey = new MultipleDataEntry();
		MultipleDataEntry multipleData = new MultipleDataEntry();
		for (int i=0;i<1000;i++){
			if (i%10000==0)
				System.out.println(i);
			DatabaseEntry key = new DatabaseEntry(("key"+i).getBytes());
			//multipleKey.append(key);
			DatabaseEntry data = new DatabaseEntry(("datadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadata"+i).getBytes());
			//database.put(transaction, key, data);
			pairSet.append(key, data);
		}
		database.putMultipleKey(transaction, pairSet, true);
	}
	
	private static void fullScan(Transaction transaction, Database testDatabase) throws DatabaseException{
		Cursor cursor = testDatabase.openCursor(transaction, CursorConfig.DEFAULT);
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value = new DatabaseEntry();
		int counter = 0;
		long start = System.currentTimeMillis();
		while ( cursor.getNext(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS ){
			if (counter  % 10000 == 0){
				//System.out.println(counter);
			}
			System.out.println(new String(key.getData())+" - "+new String(value.getData()));
			counter++;
		}
		System.out.println(counter);
		cursor.close();
		System.out.println(System.currentTimeMillis()-start);
	}
	
	public static void main(String[] args) throws FileNotFoundException, DatabaseException {
        // Set up the environment.
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

        
		Environment environment = new Environment(new File(path), myEnvConfig);

		DatabaseConfig config = new DatabaseConfig();
		config.setAllowCreate(true);
		config.setTransactional(true);
		config.setErrorStream(System.out);
		config.setType(DatabaseType.BTREE);


		Database testDatabase = environment.openDatabase(null,"test.db", null, config);

		Transaction transaction = environment.beginTransaction(null, TransactionConfig.DEFAULT);
		long size = 0;
		for (int i=0;i<100000;i++){
			if (i % 10000==0){
				System.out.println(i / 10000);
				environment.checkpoint(CheckpointConfig.DEFAULT);
			}
			TupleOutput tupleOutput = new TupleOutput();
			tupleOutput.writeInt(i);

			TupleOutput tupleOutput1 = new TupleOutput();
			tupleOutput1.writeString(String.valueOf(i));
			
			size+=tupleOutput.toByteArray().length + tupleOutput1.toByteArray().length;
			DatabaseEntry key = new DatabaseEntry(tupleOutput.toByteArray());
			DatabaseEntry value = new DatabaseEntry(tupleOutput1.toByteArray());
			testDatabase.put(transaction, key, key);
		}
		transaction.commit();
		testDatabase.close();
		environment.close();
		System.out.println(size);

	}

}
