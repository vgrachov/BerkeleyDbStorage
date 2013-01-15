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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.cursor.FullIndexCursor;
import org.brackit.berkeleydb.cursor.RangeIndexSearchCursor;
import org.brackit.berkeleydb.cursor.FullTableScanCursor;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.relational.api.cursor.ITupleCursor;
import org.brackit.relational.api.impl.DatabaseAccessFactory;
import org.brackit.relational.api.transaction.ITransaction;
import org.brackit.relational.api.transaction.ITransactionManager;
import org.brackit.relational.api.transaction.IsolationLevel;
import org.brackit.relational.api.transaction.TransactionException;
import org.brackit.relational.api.transaction.impl.TransactionManager;
import org.brackit.relational.metadata.Schema;
import org.brackit.relational.metadata.tuple.AtomicDate;
import org.brackit.relational.metadata.tuple.AtomicDouble;
import org.brackit.relational.metadata.tuple.AtomicString;
import org.brackit.relational.metadata.tuple.Tuple;
import org.brackit.relational.properties.RelationalStorageProperties;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sleepycat.db.DatabaseException;

public class Lineitem_Table_Iterator extends BasicTCPHTest{

	private static final Logger logger = Logger.getLogger(Lineitem_Table_Iterator.class);
	private static String tableName = "lineitem"; 
	
	@Test
	public void fullIterator() throws TransactionException{
		logger.debug("Full scan");
		ITransaction transaction = beginTransaction();
		ITupleCursor cursor = DatabaseAccessFactory.getInstance().create(tableName).getFullScanCursor(transaction);
		long start = System.currentTimeMillis();
		cursor.open();
		Tuple tuple = null;
		int i=0;
		List<Tuple> tuples = new LinkedList<Tuple>();
		while ((tuple = cursor.next())!=null){
			if (i%100000==0)
				logger.debug(i);
			//tuples.add(tuple);
			i++;
			//logger.debug(tuple);
		}
		cursor.close();
		commit(transaction);
		logger.info("Size : "+tuples.size());
		logger.info("Full scan time "+(System.currentTimeMillis()-start)+" "+i);
	}

	@Test
	public void fullIterator1() throws TransactionException{
		logger.debug("Full scan");
		ITransaction transaction = beginTransaction();
		ITupleCursor cursor = DatabaseAccessFactory.getInstance().create(tableName).getFullScanCursor(transaction);
		long start = System.currentTimeMillis();
		cursor.open();
		Tuple tuple = null;
		int i=0;
		while ((tuple = cursor.next())!=null){
			if (i%100000==0)
				logger.debug(i);
			i++;
		}
		cursor.close();
		commit(transaction);
		logger.info("Full scan time "+(System.currentTimeMillis()-start)+" "+i);
	}
	
	@Test
	public void fullIndexIterator() throws TransactionException, IOException {
		logger.debug("Full index scan");
		ITransaction transaction = beginTransaction();
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(tableName);
		ITupleCursor cursor = DatabaseAccessFactory.getInstance().create(tableName).getFullIndexCursor(schema.getColumns()[10], transaction);
		long start = System.currentTimeMillis();
		cursor.open();
		Tuple tuple = null;
		int i=0;
		while ((tuple = cursor.next())!=null){
			if (i%100000==0)
				logger.debug(i);
			i++;
		}
		cursor.close();
		commit(transaction);
		logger.info("Full scan time "+(System.currentTimeMillis()-start)+" "+i);
	}

	@Test
	public void rangeSearch() throws TransactionException{
		logger.debug("Range index scan");
		ITransaction transaction = beginTransaction();
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(tableName);
		SimpleDateFormat dateFormat = new SimpleDateFormat(RelationalStorageProperties.getDatePattern());
		AtomicDouble leftRange = new AtomicDouble("l_discount", 0.02);
		AtomicDouble rightRange = new AtomicDouble("l_discount", 0.03);
		ITupleCursor cursor = DatabaseAccessFactory.getInstance().create(tableName).getRangeIndexScanCursor(schema.getColumns()[6], leftRange, rightRange, transaction);
		long start = System.currentTimeMillis();
		cursor.open();
		Tuple tuple = null;
		int i=0;
		while ((tuple = cursor.next())!=null){
			if (i%10000==0)
				logger.debug(i);
			i++;
			logger.debug(tuple.getFields()[6]);
		}
		cursor.close();
		commit(transaction);
		logger.info("Full scan time "+(System.currentTimeMillis()-start)+" "+i);
	}


	@Test
	public void rangeIteratorShipdate() throws TransactionException, ParseException {
		logger.debug("Range index scan");
		ITransaction transaction = beginTransaction();
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(tableName);
		SimpleDateFormat dateFormat = new SimpleDateFormat(RelationalStorageProperties.getDatePattern());
		AtomicDate rightRange = new AtomicDate("l_shipdate", dateFormat.parse("1994-06-27").getTime());
		AtomicDate leftRange = new AtomicDate("l_shipdate", dateFormat.parse("1993-06-27").getTime());
		ITupleCursor cursor = DatabaseAccessFactory.getInstance().create(tableName).getRangeIndexScanCursor(schema.getColumns()[10], leftRange, rightRange, transaction);
		long start = System.currentTimeMillis();
		cursor.open();
		Tuple tuple = null;
		int i=0;
		while ((tuple = cursor.next())!=null){
			if (i%10000==0)
				logger.debug(i);
			i++;
		}
		cursor.close();
		commit(transaction);
		logger.info("Full scan time "+(System.currentTimeMillis()-start)+" "+i);
	}
	
	
	@AfterClass
	public static void close(){
		BerkeleyDBEnvironment.getInstance().close();
	}
	
}
