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

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.exception.KeyDuplicationException;
import org.brackit.relational.api.IDatabaseAccess;
import org.brackit.relational.api.cursor.ITupleCursor;
import org.brackit.relational.api.impl.DatabaseAccessFactory;
import org.brackit.relational.api.transaction.ITransaction;
import org.brackit.relational.api.transaction.TransactionException;
import org.brackit.relational.metadata.Schema;
import org.brackit.relational.metadata.tuple.AtomicDouble;
import org.brackit.relational.metadata.tuple.AtomicInteger;
import org.brackit.relational.metadata.tuple.AtomicValue;
import org.brackit.relational.metadata.tuple.Column;
import org.brackit.relational.metadata.tuple.ColumnType;
import org.brackit.relational.metadata.tuple.Tuple;
import org.brackit.relational.properties.RelationalStorageProperties;
import org.junit.Assert;

import com.google.common.collect.ImmutableSet;

public class Partsupp_Table_Create extends BasicTPCHFiller {

	private static final String tableName = "partsupp";
	private static final Logger logger = Logger.getLogger(Partsupp_Table_Create.class);
	
	@Override
	public void createTable(){
		Schema schema = new Schema(new Column[] {
				new Column(tableName,"ps_partkey", ColumnType.Integer,true,true),
				new Column(tableName,"ps_suppkey", ColumnType.Integer,true,true),
				new Column(tableName,"ps_availqty", ColumnType.Integer,false,false),
				new Column(tableName,"ps_supplycost", ColumnType.Double,false,false),
				//new Column(tableName,"ps_comment", ColumnType.String,false,false)
		}, tableName);
		try {
			catalog.createDatabase(schema);
		} catch (KeyDuplicationException e) {
			logger.error(e.getMessage());
			Assert.fail(e.getMessage());
		}
		Schema checkSchema = catalog.getSchemaByDatabaseName(tableName);
		for (int i=0;i<checkSchema.getColumns().length;i++) {
			logger.debug("Print column "+checkSchema.getColumns()[i]);
		}
		Assert.assertEquals(checkSchema.getDatabaseName(), tableName);
		Assert.assertEquals(checkSchema.getColumns().length, 4);
		Assert.assertEquals(checkSchema.getColumns()[0].getColumnName(), "PS_PARTKEY".toLowerCase());
		Assert.assertEquals(checkSchema.getColumns()[1].getColumnName(), "PS_SUPPKEY".toLowerCase());
		Assert.assertEquals(checkSchema.getColumns()[1].isDirectIndexExist(), true);
		Assert.assertEquals(checkSchema.getColumns()[3].getColumnName(), "PS_SUPPLYCOST".toLowerCase());
	}

	@Override
	public void fillTable() throws TransactionException{
		ITransaction transaction = beginTransaction();
		
		IDatabaseAccess databaseAccess = DatabaseAccessFactory.getInstance().create(tableName);
		BufferedReader lineItemInput = null;
		try {
			lineItemInput = new BufferedReader( new FileReader(RelationalStorageProperties.getTBLPath()+"partsupp.tbl"));
		} catch (FileNotFoundException e) {
			Assert.fail(e.getMessage());
		}
		String line = null;
		int readLines=0;
		try {
			while ((line=lineItemInput.readLine())!=null) {
				if (readLines%1000==0)
					logger.debug(readLines);
				readLines++;
				String[] entries = line.split("\\|");
				AtomicValue[] fields = new AtomicValue[4];
				fields[0] = new AtomicInteger("ps_partkey", Integer.valueOf(entries[0]));
				fields[1] = new AtomicInteger("ps_suppkey", Integer.valueOf(entries[1]));
				fields[2] = new AtomicInteger("ps_availqty", Integer.valueOf(entries[2]));
				fields[3] = new AtomicDouble("ps_supplycost", Double.valueOf(entries[3]));
				//fields[4] = new AtomicString("ps_comment", entries[4]);
				Tuple tuple = new Tuple(fields);
				databaseAccess.insert(tuple,transaction);
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
			Assert.fail(e.getMessage());
		}
		commit(transaction);
		transaction = beginTransaction();
		ITupleCursor cursor = DatabaseAccessFactory.getInstance().create(tableName).getFullScanCursor(transaction,
				ImmutableSet.of("ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost"));
		cursor.open();
		int counter = 0;
		Tuple tuple = null;
		while((tuple=cursor.next())!=null) {
			if (counter%1000==0)
				logger.debug(counter);
			counter++;
		}
		cursor.close();
		commit(transaction);
		
		logger.debug("Rows inserted "+counter);
		Assert.assertEquals(counter, readLines);
	}

	public String getTableName() {
		return tableName;
	}
}
