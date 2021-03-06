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
package org.brackit.berkeleydb.tpch;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.exception.KeyDuplicationException;
import org.brackit.relational.api.IDatabaseAccess;
import org.brackit.relational.api.cursor.ITupleCursor;
import org.brackit.relational.api.impl.DatabaseAccessFactory;
import org.brackit.relational.api.transaction.ITransaction;
import org.brackit.relational.api.transaction.TransactionException;
import org.brackit.relational.metadata.Schema;
import org.brackit.relational.metadata.tuple.AtomicChar;
import org.brackit.relational.metadata.tuple.AtomicDate;
import org.brackit.relational.metadata.tuple.AtomicDouble;
import org.brackit.relational.metadata.tuple.AtomicInteger;
import org.brackit.relational.metadata.tuple.AtomicString;
import org.brackit.relational.metadata.tuple.AtomicValue;
import org.brackit.relational.metadata.tuple.Column;
import org.brackit.relational.metadata.tuple.ColumnType;
import org.brackit.relational.metadata.tuple.Tuple;
import org.brackit.relational.properties.RelationalStorageProperties;
import org.junit.Assert;

import com.google.common.collect.ImmutableSet;

public class Lineitem_Table_Create extends BasicTPCHFiller {

	private static final String tableName = "lineitem";
	private Logger logger = Logger.getLogger(Lineitem_Table_Create.class);

	@Override
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
				new Column("lineitem", "l_shipdate", ColumnType.Date, false, true),
				new Column("lineitem", "l_commitdate", ColumnType.Date, false, false),
				new Column("lineitem", "l_receiptdate", ColumnType.Date, false, true),
				new Column("lineitem", "l_shipinstruct", ColumnType.String, false, false),
				new Column("lineitem", "l_shipmode", ColumnType.String, false, true),
				new Column("lineitem", "l_comment", ColumnType.String, false, false)
				
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
	
	@Override
	public void fillTable() throws TransactionException{
		IDatabaseAccess databaseAccess = DatabaseAccessFactory.getInstance().create("lineitem");
		BufferedReader lineItemInput = null;
		try {
			lineItemInput = new BufferedReader( new FileReader(RelationalStorageProperties.getTBLPath()+"lineitem.tbl"));
		} catch (FileNotFoundException e) {
			Assert.fail(e.getMessage());
		}
		
		String line = null;
		ITransaction transaction = beginTransaction();
		SimpleDateFormat dateFormat = new SimpleDateFormat(RelationalStorageProperties.getDatePattern());
		long start = System.currentTimeMillis();
		try {
			int i=0;
			while ((line=lineItemInput.readLine())!=null){
				if (i%1000==0)
					logger.debug(i);
				i++;
				String[] entries = line.split("\\|");
				AtomicValue[] fields = new AtomicValue[16];
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
				try{
					fields[10] = new AtomicDate("l_shipdate", dateFormat.parse(entries[10]).getTime());
					fields[11] = new AtomicDate("l_commitdate", dateFormat.parse(entries[11]).getTime());
					fields[12] = new AtomicDate("l_receiptdate", dateFormat.parse(entries[12]).getTime());
				}catch (Exception e) {
					logger.error(e.getMessage());
				}
				fields[13] = new AtomicString("l_shipinstruct", entries[13]);
				fields[14] = new AtomicString("l_shipmode", entries[14]);
				fields[15] = new AtomicString("l_comment", entries[15]);
				Tuple tuple = new Tuple(fields);
				databaseAccess.insert(tuple,transaction);
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		try {
			if (transaction != null)
				transaction.commit();
		} catch (TransactionException e) {
			logger.fatal(e.getMessage());
		}
		System.out.println("Insert time "+(System.currentTimeMillis() - start));
		ITupleCursor cursor = DatabaseAccessFactory.getInstance().create("lineitem").getFullScanCursor(transaction, 
				ImmutableSet.of("l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", 
						"l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"));
		cursor.open();
		int counter = 0;
		Tuple tuple = null;
		while((tuple =cursor.next())!=null){
			counter++;
		}
		cursor.close();
		logger.debug("Rows : "+counter);
		
	}
	
	public String getTableName() {
		return tableName;
	}
}
