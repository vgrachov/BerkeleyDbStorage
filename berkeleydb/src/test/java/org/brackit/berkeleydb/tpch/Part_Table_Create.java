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
import org.brackit.berkeleydb.environment.IBerkeleyDBEnvironment;
import org.brackit.berkeleydb.exception.KeyDuplicationException;
import org.brackit.berkeleydb.tuple.Atomic;
import org.brackit.berkeleydb.tuple.AtomicChar;
import org.brackit.berkeleydb.tuple.AtomicDouble;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;
import org.brackit.berkeleydb.tuple.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class Part_Table_Create {

	private static ICatalog catalog;
	private static final String tableName = "part";
	private static final Logger logger = Logger.getLogger(Part_Table_Create.class);
	private static IBerkeleyDBEnvironment berkeleyDBEnvironment;
	
	@BeforeClass
	public static void init(){
		berkeleyDBEnvironment = BerkeleyDBEnvironment.getInstance();
		catalog = Catalog.getInstance();
	}

	@Test
	public void createTable(){
		catalog = Catalog.getInstance();
		Schema schema = new Schema(new Column[]{
				new Column(tableName,"p_partkey", ColumnType.Integer,true,true),
				new Column(tableName,"p_name", ColumnType.String,false,true),
				new Column(tableName,"p_mfgr", ColumnType.String,false,false),
				new Column(tableName,"p_brand", ColumnType.String,false,true),
				new Column(tableName,"p_type", ColumnType.String,false,false),
				new Column(tableName,"p_size", ColumnType.Integer,false,false),
				new Column(tableName,"p_container", ColumnType.String,false,false),
				new Column(tableName,"p_retailprice", ColumnType.Double,false,false),
				new Column(tableName,"p_comment", ColumnType.String,false,false)
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
		Assert.assertEquals(schema2.getColumns().length, 9);
		Assert.assertEquals(schema2.getColumns()[0].getColumnName(), "P_PARTKEY".toLowerCase());
		Assert.assertEquals(schema2.getColumns()[2].getColumnName(), "P_MFGR".toLowerCase());
		Assert.assertEquals(schema2.getColumns()[1].isDirectIndexExist(), true);
		Assert.assertEquals(schema2.getColumns()[7].getColumnName(), "P_RETAILPRICE".toLowerCase());
	}

	@Test
	public void fillTable(){
		IDatabaseAccess databaseAccess = new DatabaseAccess(tableName);
		//BufferedReader lineItemInput = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResourceAsStream("tpc-h/100KB_data/lineitem.tbl")));
		BufferedReader lineItemInput = null;
		try {
			lineItemInput = new BufferedReader( new FileReader("G:\\Projects\\tpch\\10MB\\part.tbl"));
		} catch (FileNotFoundException e) {
			Assert.fail(e.getMessage());
		}
		String line = null;
		try {
			int i=0;
			while ((line=lineItemInput.readLine())!=null){
				String[] entries = line.split("\\|");
				Atomic[] fields = new Atomic[9];
				fields[0] = new AtomicInteger("p_partkey", Integer.valueOf(entries[0]));
				fields[1] = new AtomicString("p_name", entries[1]);
				fields[2] = new AtomicString("p_mfgr", entries[2]);
				fields[3] = new AtomicString("p_brand", entries[3]);
				fields[4] = new AtomicString("p_type", entries[4]);
				fields[5] = new AtomicInteger("p_size", Integer.valueOf(entries[5]));
				fields[6] = new AtomicString("p_container", entries[6]);
				fields[7] = new AtomicDouble("p_retailprice", Double.valueOf(entries[7]));
				fields[8] = new AtomicString("p_comment", entries[8]);
				Tuple tuple = new Tuple(fields);
				databaseAccess.insert(tuple);
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
			Assert.fail(e.getMessage());
		}
		ITupleCursor cursor = new FullTableScanCursor(tableName);
		cursor.open();
		int counter = 0;
		Tuple tuple = null;
		while((tuple=cursor.next())!=null){
			counter++;
		}
		cursor.close();
		Assert.assertEquals(counter, 2000);
	}
	
	@AfterClass
	public static void close(){
		berkeleyDBEnvironment.close();
	}

	
}
