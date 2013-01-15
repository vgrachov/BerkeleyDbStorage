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
package org.brackit.berkeleydb.cursor;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.transaction.BerkeleydbTransaction;
import org.brackit.relational.api.IDatabaseAccess;
import org.brackit.relational.api.cursor.ITupleCursor;
import org.brackit.relational.api.transaction.ITransaction;
import org.brackit.relational.metadata.Schema;
import org.brackit.relational.metadata.tuple.AtomicValue;
import org.brackit.relational.metadata.tuple.Column;
import org.brackit.relational.metadata.tuple.Tuple;

import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.OperationStatus;

public class BerkeleydbDatabaseAccess implements IDatabaseAccess {

	private static final Logger logger = Logger.getLogger(BerkeleydbDatabaseAccess.class);
	
	protected final Database dataBase;
	protected final Schema schema;
	protected final RelationalTupleBinding tupleBinding;
	public static int sizeValue = 0;
	public static int sizeKey = 0;
	
	public BerkeleydbDatabaseAccess(String databaseName){
		this.dataBase = BerkeleyDBEnvironment.getInstance().getDatabasereference(databaseName);
		this.schema = Catalog.getInstance().getSchemaByDatabaseName(databaseName);
		this.tupleBinding = new RelationalTupleBinding(schema.getColumns());

	}
	
	private boolean validation(ITransaction transaction){
		if (transaction==null || transaction instanceof BerkeleydbTransaction )
			return true;
		else 
			return false;
	}
	
	public boolean insert(Tuple tuple, ITransaction transaction) {
		boolean validation = validation(transaction);
		if (!validation)
			return false;
		TupleOutput serializedTupleValues = new TupleOutput();
		TupleOutput serializedKey = new TupleOutput();
		tupleBinding.smartObjectToEntry(tuple, serializedKey, serializedTupleValues);
		sizeKey += serializedKey.toByteArray().length;
		sizeValue += serializedTupleValues.toByteArray().length;
		try {
			//TODO:review put 
			OperationStatus status = null;
			if (transaction == null)
				status = dataBase.put(null, new DatabaseEntry(serializedKey.toByteArray()), new DatabaseEntry(serializedTupleValues.toByteArray()));
			else
				status = dataBase.put(((BerkeleydbTransaction)transaction).getTransaction(), new DatabaseEntry(serializedKey.toByteArray()), new DatabaseEntry(serializedTupleValues.toByteArray()));
			if (status != OperationStatus.SUCCESS)
				logger.debug(status);
		} catch (DatabaseException e) {
			logger.error(e.getMessage());
			return false;
			//TODO: exception handling
		}
		return true;
	}
	
	
	public boolean delete(Tuple tuple, ITransaction transaction){
		boolean validation = validation(transaction);
		if (!validation)
			return false;

		TupleOutput serializedTupleValue = new TupleOutput();
		TupleOutput serializedKey = new TupleOutput();
		tupleBinding.smartObjectToEntry(tuple, serializedKey, serializedTupleValue);
		try {
			OperationStatus status = null;
			if (transaction == null)
				status = dataBase.delete(null, new DatabaseEntry(serializedKey.toByteArray()));
			else
				status = dataBase.delete(((BerkeleydbTransaction)transaction).getTransaction(), new DatabaseEntry(serializedKey.toByteArray()));
			if (status == OperationStatus.SUCCESS)
				return true;
			else
				return false;
		} catch (DatabaseException e) {
			logger.error(e.getMessage());
		}
		return false;
	}


	public ITupleCursor getFullScanCursor(ITransaction transaction) {
		boolean validation = validation(transaction);
		if (!validation)
			return null;
		FullTableScanCursor fullTableScanCursor = null;
		if (transaction == null)
			fullTableScanCursor = new FullTableScanCursor(schema.getDatabaseName(),null);
		else
			fullTableScanCursor = new FullTableScanCursor(schema.getDatabaseName(),((BerkeleydbTransaction)transaction).getTransaction());
		return fullTableScanCursor;
	}

	public ITupleCursor getRangeIndexScanCursor(Column column, AtomicValue leftKey, AtomicValue rightKey,ITransaction transaction) {
		boolean validation = validation(transaction);//TODO: check whether index exists
		if (!validation)
			return null;
		RangeIndexSearchCursor rangeIndexSearchCursor = null;
		if (transaction == null)
			rangeIndexSearchCursor = new RangeIndexSearchCursor(column,leftKey,rightKey,null);
		else
			rangeIndexSearchCursor = new RangeIndexSearchCursor(column,leftKey,rightKey,((BerkeleydbTransaction)transaction).getTransaction());
		return rangeIndexSearchCursor;
	}

	public ITupleCursor getFullIndexCursor(Column column, ITransaction transaction) {
		boolean validation = validation(transaction);//TODO: check whether index exists
		if (!validation)
			return null;
		FullIndexCursor fullIndexCursor = null;
		if (transaction == null)
			fullIndexCursor = new FullIndexCursor(column,null);
		else
			fullIndexCursor = new FullIndexCursor(column,((BerkeleydbTransaction)transaction).getTransaction());
		return fullIndexCursor;
	}

	public ITupleCursor getEqualMatchIndexSearchCursor(Column column, AtomicValue value, ITransaction transaction) {
		boolean validation = validation(transaction);//TODO: check whether index exists
		if (!validation)
			return null;
		EqualMatchIndexSearchCursor equalMatchIndexSearchCursor = null;
		if (transaction == null)
			equalMatchIndexSearchCursor = new EqualMatchIndexSearchCursor(schema.getDatabaseName(), column, value, null);
		else
			equalMatchIndexSearchCursor = new EqualMatchIndexSearchCursor(schema.getDatabaseName(), column, value,((BerkeleydbTransaction)transaction).getTransaction());
		return equalMatchIndexSearchCursor;
	}

	
}
