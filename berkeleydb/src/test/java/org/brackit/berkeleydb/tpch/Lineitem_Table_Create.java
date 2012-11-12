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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.DatabaseAccess;
import org.brackit.berkeleydb.IDatabaseAccess;
import org.brackit.berkeleydb.Schema;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.exception.KeyDuplicationException;
import org.brackit.berkeleydb.tuple.Atomic;
import org.brackit.berkeleydb.tuple.AtomicChar;
import org.brackit.berkeleydb.tuple.AtomicDate;
import org.brackit.berkeleydb.tuple.AtomicDouble;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;
import org.brackit.berkeleydb.tuple.Tuple;
import org.junit.AfterClass;
import org.junit.Test;

public class Lineitem_Table_Create {

	private Logger logger = Logger.getLogger(Lineitem_Table_Create.class);

	@Test
	public void createTable(){
		logger.debug("Create lineitem table");
		Column[] columns = new Column[]{
				new Column("lineitem", "l_orderkey", ColumnType.Integer, true, false),
				new Column("lineitem", "l_partkey", ColumnType.Integer, false, true),
				new Column("lineitem", "l_suppkey", ColumnType.Integer, false, true),
				new Column("lineitem", "l_linenumber", ColumnType.Integer, true, true),
				new Column("lineitem", "l_quantity", ColumnType.Double, false, false),
				new Column("lineitem", "l_extendedprice", ColumnType.Double, false, false),
				new Column("lineitem", "l_discount", ColumnType.Double, false, true),
				new Column("lineitem", "l_tax", ColumnType.Double, false, false),
				new Column("lineitem", "l_returnflag", ColumnType.Char, false, false),
				new Column("lineitem", "l_linestatus", ColumnType.Char, false, false),
				new Column("lineitem", "l_shipdate", ColumnType.String, false, true),
				new Column("lineitem", "l_commitdate", ColumnType.String, false, false),
				new Column("lineitem", "l_receiptdate", ColumnType.String, false, false),
				new Column("lineitem", "l_shipinstruct", ColumnType.String, false, false),
				new Column("lineitem", "l_shipmode", ColumnType.String, false, true),
				new Column("lineitem", "l_comment", ColumnType.String, false, false),
				
		};
		Schema schema = new Schema(columns, "lineitem");
		try{
			Catalog.getInstance().createDatabase(schema);
		} catch (KeyDuplicationException e) {
			logger.error(e.getMessage());
			Assert.fail(e.getMessage());
		}
		logger.debug("Table create is finish");
	}
	
	@Test
	public void fillTable(){
		IDatabaseAccess databaseAccess = new DatabaseAccess("lineitem");
		//BufferedReader lineItemInput = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResourceAsStream("tpc-h/100KB_data/lineitem.tbl")));
		BufferedReader lineItemInput = null;
		try {
			//lineItemInput = new BufferedReader( new FileReader("G:\\Projects\\tpch\\10mb\\lineitem.tbl"));
			lineItemInput = new BufferedReader( new FileReader("G:\\Projects\\tpch\\10mb\\lineitem.tbl"));
			
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		String line = null;
		try {
			int i=0;
			while ((line=lineItemInput.readLine())!=null){
				String[] entries = line.split("\\|");
				Atomic[] fields = new Atomic[16];
				fields[0] = new AtomicInteger("l_orderkey", Integer.valueOf(entries[0]));
				fields[1] = new AtomicInteger("l_partkey", Integer.valueOf(entries[1]));
				fields[2] = new AtomicInteger("l_suppkey", Integer.valueOf(entries[2]));
				fields[3] = new AtomicInteger("l_linenumber", Integer.valueOf(entries[3]));
				fields[4] = new AtomicDouble("l_quantity", Double.valueOf(entries[4]));
				fields[5] = new AtomicDouble("l_extendedprice", Double.valueOf(entries[5]));
				fields[6] = new AtomicDouble("l_discount", Double.valueOf(entries[6]));
				fields[7] = new AtomicDouble("l_tax", Double.valueOf(entries[7]));
				fields[8] = new AtomicChar("l_returnflag", entries[8].charAt(0));
				fields[9] = new AtomicChar("l_linestatus", entries[9].charAt(0));
				/*try{
					fields[10] = new AtomicDate("L_SHIPDATE", dateFormat.parse(entries[10]));
					fields[11] = new AtomicDate("L_COMMITDATE", dateFormat.parse(entries[11]));
					fields[12] = new AtomicDate("L_RECEIPTDATE", dateFormat.parse(entries[12]));
				}catch (Exception e) {
					logger.error(e.getMessage());
				}*/

				fields[10] = new AtomicString("l_shipdate", entries[10]);
				fields[11] = new AtomicString("l_commitdate", entries[11]);
				fields[12] = new AtomicString("l_receiptdate", entries[12]);

				fields[13] = new AtomicString("l_shipinstruct", entries[13]);
				fields[14] = new AtomicString("l_shipmode", entries[14]);
				fields[15] = new AtomicString("l_comment", entries[15]);
				Tuple tuple = new Tuple(fields);
				databaseAccess.insert(tuple);
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	@AfterClass
	public static void close(){
		BerkeleyDBEnvironment.getInstance().close();
	}
	
}
