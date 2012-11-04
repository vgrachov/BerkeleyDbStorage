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
package org.brackit.berkeleydb.cursor;

import java.util.Arrays;
import java.util.Date;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.Catalog;
import org.brackit.berkeleydb.Schema;
import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.impl.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.Atomic;
import org.brackit.berkeleydb.tuple.AtomicChar;
import org.brackit.berkeleydb.tuple.AtomicDate;
import org.brackit.berkeleydb.tuple.AtomicDouble;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;
import org.brackit.berkeleydb.tuple.DatabaseBindingHelper;
import org.brackit.berkeleydb.tuple.DatabaseEntryHelper;
import org.brackit.berkeleydb.tuple.Tuple;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;

public class RangeIndexSearchCursor implements ITupleCursor {

	private static final Logger logger = Logger.getLogger(RangeIndexSearchCursor.class);
	
	private Column column;
	private SecondaryCursor cursor;

	private DatabaseEntry currentSecondaryIndexKey;
	private DatabaseEntry currentPrimaryKey;
	private DatabaseEntry currentPrimaryValue;
	
	private OperationStatus retVal = OperationStatus.NOTFOUND;
	
	private final RelationalTupleBinding tupleBinding;
	private final Atomic rightKey;
	private final Atomic leftKey;
	private final EntryBinding binding;
	
	public RangeIndexSearchCursor(Column column, Atomic leftKey, Atomic rightKey){
		logger.debug("Create range search cursor for table "+column.getDatabaseName()+" and column"+column.getColumnName());
		logger.debug("Left range "+leftKey);
		logger.debug("Right range "+rightKey);
		this.rightKey = rightKey;
		this.leftKey = leftKey;
		currentSecondaryIndexKey = DatabaseEntryHelper.getInstance().createDatabaseEntry(leftKey);
		this.column = column;
		binding = DatabaseBindingHelper.getInstance().databaseBinding(column.getType());
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(column.getDatabaseName());
		tupleBinding = new RelationalTupleBinding(schema.getColumns());
	}
	
	public void open() {
		SecondaryDatabase database = BerkeleyDBEnvironment.getInstance().getIndexreference(column);
		cursor = database.openCursor(null, null);
		currentPrimaryKey = new DatabaseEntry();
		currentPrimaryValue = new DatabaseEntry();
	}

	private int compare(DatabaseEntry currentSecondaryIndexKey){
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
			Date currentSecondaryKey = (Date)binding.entryToObject(currentSecondaryIndexKey);
			compare = ((AtomicDate)rightKey).getData().compareTo(currentSecondaryKey);
		}
		return compare;
	}
	
	public Tuple next() {
		if (retVal == OperationStatus.NOTFOUND){
			retVal = cursor.getSearchKeyRange(currentSecondaryIndexKey, currentPrimaryKey, currentPrimaryValue, LockMode.DEFAULT);
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
			retVal = cursor.getNextDup(currentSecondaryIndexKey, currentPrimaryKey, currentPrimaryValue, LockMode.DEFAULT);
			if (retVal == OperationStatus.SUCCESS){
				TupleInput foundKeySerialized = new TupleInput(currentPrimaryKey.getData());
				TupleInput foundDataSerialized = new TupleInput(currentPrimaryValue.getData());
				Tuple tuple = tupleBinding.smartEntryToObject(foundKeySerialized, foundDataSerialized);
				return tuple;
			}else{
				retVal = cursor.getNext(currentSecondaryIndexKey, currentPrimaryKey, currentPrimaryValue, LockMode.DEFAULT);
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
		cursor.close();
		
	}

}
