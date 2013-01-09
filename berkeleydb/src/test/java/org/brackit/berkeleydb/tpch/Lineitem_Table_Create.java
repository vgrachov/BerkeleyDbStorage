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
import org.brackit.berkeleydb.cursor.FullTableScanCursor;
import org.brackit.berkeleydb.cursor.ITupleCursor;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.exception.KeyDuplicationException;
import org.brackit.berkeleydb.tuple.AtomicValue;
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

import com.sleepycat.db.DatabaseException;

public class Lineitem_Table_Create {

	private Logger logger = Logger.getLogger(Lineitem_Table_Create.class);

	@Test
	public void createTable(){
		logger.debug("Create lineitem table");
		Column[] columns = new Column[]{
				new Column("lineitem", "l_orderkey", ColumnType.Integer, true, true),
				new Column("lineitem", "l_partkey", ColumnType.Integer, false, true),
				new Column("lineitem", "l_suppkey", ColumnType.Integer, false, true),
				new Column("lineitem", "l_linenumber", ColumnType.Integer, true, true),
				new Column("lineitem", "l_quantity", ColumnType.Double, false, false),
				new Column("lineitem", "l_extendedprice", ColumnType.Double, false, false),
				new Column("lineitem", "l_discount", ColumnType.Double, false, true),
				new Column("lineitem", "l_tax", ColumnType.Double, false, false),
				new Column("lineitem", "l_returnflag", ColumnType.Char, false, true),
				new Column("lineitem", "l_linestatus", ColumnType.Char, false, false),
				new Column("lineitem", "l_shipdate", ColumnType.String, false, true),
				new Column("lineitem", "l_commitdate", ColumnType.String, false, false),
				new Column("lineitem", "l_receiptdate", ColumnType.String, false, true),
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
			lineItemInput = new BufferedReader( new FileReader("E:\\tpch\\10mb\\lineitem.tbl"));
			
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		String line = null;
		try {
			int i=0;
			while ((line=lineItemInput.readLine())!=null){
				if (i%1000==0)
					logger.debug(i);
				i++;
				String[] entries = line.split("\\|");
				AtomicValue[] fields = new AtomicValue[16];
				//Atomic[] fields = new Atomic[4];
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
		ITupleCursor cursor = new FullTableScanCursor("lineitem");
		cursor.open();
		int counter = 0;
		Tuple tuple = null;
		while((tuple =cursor.next())!=null){
			counter++;
		}
		cursor.close();
		logger.debug("Rows : "+counter);
		
	}
	
	@AfterClass
	public static void close(){
		BerkeleyDBEnvironment.getInstance().close();
	}
	
}
