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
package org.brackit.berkeleydb.tpch;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.DatabaseAccess;
import org.brackit.berkeleydb.IDatabaseAccess;
import org.brackit.berkeleydb.Schema;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.catalog.ICatalog;
import org.brackit.berkeleydb.cursor.FullTableScanCursor;
import org.brackit.berkeleydb.cursor.ITupleCursor;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.exception.KeyDuplicationException;
import org.brackit.berkeleydb.tuple.Atomic;
import org.brackit.berkeleydb.tuple.AtomicChar;
import org.brackit.berkeleydb.tuple.AtomicDouble;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;
import org.brackit.berkeleydb.tuple.Tuple;
import org.junit.BeforeClass;
import org.junit.Test;

public class Customer_Table_Create {
	
	private static final Logger logger = Logger.getLogger(Customer_Table_Create.class);
	private static final String tableName = "customer";
	
	private static ICatalog catalog;
	
	@BeforeClass
	public static void init(){
		catalog = Catalog.getInstance();
	}
	
	@Test
	public void deleteCreateDeleteCreateTest(){
		boolean delete = deleteTable();
		createTable();
		delete = deleteTable();
		Assert.assertEquals(delete, true);
		createTable();
	}

	public void createTable(){
		catalog = Catalog.getInstance();
		Schema schema = new Schema(new Column[]{
				new Column(tableName,"c_custkey", ColumnType.Integer,true,true),
				new Column(tableName,"c_name", ColumnType.String,false,true),
				new Column(tableName,"c_address", ColumnType.String,false,false),
				new Column(tableName,"c_nationkey", ColumnType.Integer,false,true),
				new Column(tableName,"c_phone", ColumnType.String,false,false),
				new Column(tableName,"c_acctbal", ColumnType.Double,false,false),
				new Column(tableName,"c_mktsegment", ColumnType.String,false,true),
				new Column(tableName,"c_comment", ColumnType.String,false,false)
		}, tableName);
		try{
			catalog.createDatabase(schema);
		} catch (KeyDuplicationException e) {
			logger.error(e.getMessage());
			Assert.fail(e.getMessage());
		}
		Schema schema2 = catalog.getSchemaByDatabaseName(tableName);
		for (int i=0;i<schema2.getColumns().length;i++){
			logger.debug("Print column "+schema2.getColumns()[i]);
		}
		Assert.assertEquals(schema2.getDatabaseName(), tableName);
		Assert.assertEquals(schema2.getColumns().length, 8);
		Assert.assertEquals(schema2.getColumns()[0].getColumnName(), "c_custkey");
		Assert.assertEquals(schema2.getColumns()[1].isDirectIndexExist(), true);
		Assert.assertEquals(schema2.getColumns()[7].getColumnName(), "c_comment");
	}
	
	
	public boolean deleteTable(){
		boolean delete = catalog.deleteDatabase(tableName);
		return delete;
	}
	
	@Test
	public void fillTable(){
		IDatabaseAccess databaseAccess = new DatabaseAccess(tableName);
		//BufferedReader lineItemInput = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResourceAsStream("tpc-h/100KB_data/lineitem.tbl")));
		BufferedReader lineItemInput = null;
		try {
			lineItemInput = new BufferedReader( new FileReader("G:\\Projects\\tpch\\10mb\\customer.tbl"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String line = null;
		try {
			int i=0;
			while ((line=lineItemInput.readLine())!=null){
				String[] entries = line.split("\\|");
				Atomic[] fields = new Atomic[8];
				fields[0] = new AtomicInteger("c_custkey", Integer.valueOf(entries[0]));
				fields[1] = new AtomicString("c_name", entries[1]);
				fields[2] = new AtomicString("c_address", entries[2]);
				fields[3] = new AtomicInteger("c_nationkey", Integer.valueOf(entries[3]));
				fields[4] = new AtomicString("c_phone", entries[4]);
				fields[5] = new AtomicDouble("c_acctbal", Double.valueOf(entries[5]));
				fields[6] = new AtomicString("c_mktsegment", entries[6]);
				fields[7] = new AtomicString("c_comment", entries[7]);
				Tuple tuple = new Tuple(fields);
				databaseAccess.insert(tuple);
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		ITupleCursor cursor = new FullTableScanCursor(tableName);
		cursor.open();
		int counter = 0;
		while(cursor.next()!=null)
			counter++;
		cursor.close();
		Assert.assertEquals(counter, 1500);
	}

	
	public static void close(){
		BerkeleyDBEnvironment.getInstance().close();
	}
	
}
