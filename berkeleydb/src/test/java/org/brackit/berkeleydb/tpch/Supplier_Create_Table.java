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
import org.brackit.berkeleydb.tuple.AtomicValue;
import org.brackit.berkeleydb.tuple.AtomicDouble;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;
import org.brackit.berkeleydb.tuple.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class Supplier_Create_Table {

	private static ICatalog catalog;
	private static final String tableName = "supplier";
	private static final Logger logger = Logger.getLogger(Supplier_Create_Table.class);
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
				new Column(tableName,"s_suppkey", ColumnType.Integer,true,true),
				new Column(tableName,"s_name", ColumnType.String,false,true),
				new Column(tableName,"s_address", ColumnType.String,false,false),
				new Column(tableName,"s_nationkey", ColumnType.Integer,false,true),
				new Column(tableName,"s_phone", ColumnType.String,false,false),
				new Column(tableName,"s_acctbal", ColumnType.Double,false,false),
				new Column(tableName,"s_comment", ColumnType.String,false,false)
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
		Assert.assertEquals(schema2.getColumns().length, 7);
		Assert.assertEquals(schema2.getColumns()[0].getColumnName(), "S_SUPPKEY".toLowerCase());
		Assert.assertEquals(schema2.getColumns()[1].getColumnName(), "S_NAME".toLowerCase());
		Assert.assertEquals(schema2.getColumns()[2].getColumnName(), "S_ADDRESS".toLowerCase());
		Assert.assertEquals(schema2.getColumns()[3].getColumnName(), "S_NATIONKEY".toLowerCase());
		Assert.assertEquals(schema2.getColumns()[4].getColumnName(), "S_PHONE".toLowerCase());
		Assert.assertEquals(schema2.getColumns()[5].getColumnName(), "S_ACCTBAL".toLowerCase());
		Assert.assertEquals(schema2.getColumns()[6].getColumnName(), "S_COMMENT".toLowerCase());
		Assert.assertEquals(schema2.getColumns()[1].isDirectIndexExist(), true);
		Assert.assertEquals(schema2.getColumns()[0].isDirectIndexExist(), true);
	}

	@Test
	public void fillTable(){
		IDatabaseAccess databaseAccess = new DatabaseAccess(tableName);
		//BufferedReader lineItemInput = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResourceAsStream("tpc-h/100KB_data/lineitem.tbl")));
		BufferedReader lineItemInput = null;
		try {
			lineItemInput = new BufferedReader( new FileReader("E:\\tpch\\10mb\\supplier.tbl"));
		} catch (FileNotFoundException e) {
			Assert.fail(e.getMessage());
		}
		String line = null;
		try {
			int i=0;
			while ((line=lineItemInput.readLine())!=null){
				String[] entries = line.split("\\|");
				AtomicValue[] fields = new AtomicValue[7];
				fields[0] = new AtomicInteger("s_suppkey", Integer.valueOf(entries[0]));
				fields[1] = new AtomicString("s_name", entries[1]);
				fields[2] = new AtomicString("s_address", entries[2]);
				fields[3] = new AtomicInteger("s_nationkey", Integer.valueOf(entries[3]));
				fields[4] = new AtomicString("s_phone", entries[4]);
				fields[5] = new AtomicDouble("s_acctbal", Double.valueOf(entries[5]));
				fields[6] = new AtomicString("s_comment", entries[6]);
				Tuple tuple = new Tuple(fields);
				databaseAccess.insert(tuple);
			}
		} catch (IOException e) {
			Assert.fail(e.getMessage());
		}
		ITupleCursor cursor = new FullTableScanCursor(tableName);
		cursor.open();
		int counter = 0;
		Tuple tuple = null;
		while((tuple=cursor.next())!=null){
			logger.debug(tuple);
			counter++;
		}
		cursor.close();
		logger.debug("Row count "+counter);
		//Assert.assertEquals(counter, 100);
	}
	
	@AfterClass
	public static void close(){
		berkeleyDBEnvironment.close();
	}

	
}
