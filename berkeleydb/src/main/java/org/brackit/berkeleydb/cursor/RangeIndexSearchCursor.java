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
import org.brackit.relational.api.cursor.ITupleCursor;
import org.brackit.relational.metadata.Schema;
import org.brackit.relational.metadata.tuple.AtomicChar;
import org.brackit.relational.metadata.tuple.AtomicDate;
import org.brackit.relational.metadata.tuple.AtomicDouble;
import org.brackit.relational.metadata.tuple.AtomicInteger;
import org.brackit.relational.metadata.tuple.AtomicString;
import org.brackit.relational.metadata.tuple.AtomicValue;
import org.brackit.relational.metadata.tuple.Column;
import org.brackit.relational.metadata.tuple.ColumnType;
import org.brackit.relational.metadata.tuple.PrimitiveTypeBindingFactory;
import org.brackit.relational.metadata.tuple.DatabaseEntryFactory;
import org.brackit.relational.metadata.tuple.Tuple;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.CursorConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.SecondaryCursor;
import com.sleepycat.db.SecondaryDatabase;
import com.sleepycat.db.Transaction;

public class RangeIndexSearchCursor implements ITupleCursor {

	private static final Logger logger = Logger.getLogger(RangeIndexSearchCursor.class);
	
	private Column column;
	private SecondaryCursor cursor;

	private DatabaseEntry currentSecondaryIndexKey;
	private DatabaseEntry currentPrimaryKey;
	private DatabaseEntry currentPrimaryValue;
	
	private OperationStatus retVal = OperationStatus.NOTFOUND;
	
	private final RelationalTupleBinding tupleBinding;
	private final AtomicValue rightKey;
	private final AtomicValue leftKey;
	private final EntryBinding binding;
	private final Transaction transaction;
	
	RangeIndexSearchCursor(Column column, AtomicValue leftKey, AtomicValue rightKey){
		this(column,leftKey,rightKey,null);
	}
	
	RangeIndexSearchCursor(Column column, AtomicValue leftKey, AtomicValue rightKey, Transaction transaction){
		logger.debug("Create range search cursor for table "+column.getDatabaseName()+" and column"+column.getColumnName());
		logger.debug("Left range "+leftKey);
		logger.debug("Right range "+rightKey);
		this.rightKey = rightKey;
		this.leftKey = leftKey;
		this.column = column;
		if (leftKey!=null)
			currentSecondaryIndexKey = DatabaseEntryFactory.createDatabaseEntry(leftKey);
		else
			currentSecondaryIndexKey = null;
		this.binding = PrimitiveTypeBindingFactory.databaseBinding(column.getType());
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(column.getDatabaseName());
		this.tupleBinding = new RelationalTupleBinding(schema.getColumns());
		this.transaction = transaction;
	}
	
	public void open() {
		SecondaryDatabase database = BerkeleyDBEnvironment.getInstance().getIndexreference(column);
		try {
			cursor = database.openSecondaryCursor(transaction, CursorConfig.DEFAULT);
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		currentPrimaryKey = new DatabaseEntry();
		currentPrimaryValue = new DatabaseEntry();
	}

	private int compare(DatabaseEntry currentSecondaryIndexKey){
		if (rightKey == null)
			return 1;
		int compare = 0;
		if (column.getType() == ColumnType.String){
			String currentSecondaryKey = (String)binding.entryToObject(currentSecondaryIndexKey);
			compare = ((AtomicString)rightKey).getData().compareTo(currentSecondaryKey);
		}else
		if (column.getType() == ColumnType.Integer){
			Integer currentSecondaryKey = (Integer)binding.entryToObject(currentSecondaryIndexKey);
			compare = ((AtomicInteger)rightKey).getData().compareTo(currentSecondaryKey);
		}else
		if (column.getType() == ColumnType.Double){
			Double currentSecondaryKey = (Double)binding.entryToObject(currentSecondaryIndexKey);
			compare = ((AtomicDouble)rightKey).getData().compareTo(currentSecondaryKey);
		}else
		if (column.getType() == ColumnType.Char){
			Character currentSecondaryKey = (Character)binding.entryToObject(currentSecondaryIndexKey);
			compare = ((AtomicChar)rightKey).getData().compareTo(currentSecondaryKey);
		}else
		if (column.getType() == ColumnType.Date){
			Long currentSecondaryKey = (Long)binding.entryToObject(currentSecondaryIndexKey);
			compare = ((AtomicDate)rightKey).getData().compareTo(currentSecondaryKey);
		}
		return compare;
	}
	
	public Tuple next() {
		if (retVal == OperationStatus.NOTFOUND){
			if (currentSecondaryIndexKey!=null)
				try {
					retVal = cursor.getSearchKeyRange(currentSecondaryIndexKey, currentPrimaryKey, currentPrimaryValue, LockMode.DEFAULT);
				} catch (DatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			else{
				currentSecondaryIndexKey = new DatabaseEntry();
				try {
					retVal = cursor.getFirst(currentSecondaryIndexKey, currentPrimaryKey, currentPrimaryValue, LockMode.DEFAULT);
				} catch (DatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (retVal==OperationStatus.SUCCESS){
				int compare = compare(currentSecondaryIndexKey);
				if (compare<0)
					return null;
				TupleInput foundKeySerialized = new TupleInput(currentPrimaryKey.getData());
				TupleInput foundDataSerialized = new TupleInput(currentPrimaryValue.getData());
				Tuple tuple = tupleBinding.smartEntryToObject(foundKeySerialized, foundDataSerialized);
				return tuple;
			}else
				return null;
		}else
		if (retVal == OperationStatus.SUCCESS){
			try {
				retVal = cursor.getNextDup(currentSecondaryIndexKey, currentPrimaryKey, currentPrimaryValue, LockMode.DEFAULT);
			} catch (DatabaseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (retVal == OperationStatus.SUCCESS){
				TupleInput foundKeySerialized = new TupleInput(currentPrimaryKey.getData());
				TupleInput foundDataSerialized = new TupleInput(currentPrimaryValue.getData());
				Tuple tuple = tupleBinding.smartEntryToObject(foundKeySerialized, foundDataSerialized);
				return tuple;
			}else{
				try {
					retVal = cursor.getNext(currentSecondaryIndexKey, currentPrimaryKey, currentPrimaryValue, LockMode.DEFAULT);
				} catch (DatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (retVal==OperationStatus.NOTFOUND)
					return null;
				int compare = compare(currentSecondaryIndexKey);
				if (compare<0)
					return null;
				TupleInput foundKeySerialized = new TupleInput(currentPrimaryKey.getData());
				TupleInput foundDataSerialized = new TupleInput(currentPrimaryValue.getData());
				Tuple tuple = tupleBinding.smartEntryToObject(foundKeySerialized, foundDataSerialized);
				return tuple;
			}
		}
		return null;
	}

	public void close() {
		try {
			cursor.close();
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
