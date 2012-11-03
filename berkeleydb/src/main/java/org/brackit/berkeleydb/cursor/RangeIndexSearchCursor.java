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

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.Catalog;
import org.brackit.berkeleydb.Schema;
import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.impl.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.Atomic;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;
import org.brackit.berkeleydb.tuple.DatabaseEntryFactory;
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
	
	private DatabaseEntry leftKeyDatabaseEntry;
	private DatabaseEntry rightKeyDatabaseEntry;
	private Column column;
	private SecondaryCursor cursor;
	private DatabaseEntry currentSecondaryIndexKey;
	private DatabaseEntry lastSecondaryIndexKey;
	private DatabaseEntry currentPrimaryKey;
	private DatabaseEntry currentPrimaryValue;
	private OperationStatus retVal = OperationStatus.NOTFOUND;
	private RelationalTupleBinding tupleBinding;
	private boolean isStrictLess = false;
	private Atomic rightKey;
	private Atomic leftKey;
	private EntryBinding binding;
	
	public RangeIndexSearchCursor(Column column, Atomic leftKey, Atomic rightKey){
		logger.debug("Create range search cursor for table "+column.getDatabaseName()+" and column"+column.getColumnName());
		logger.debug("Left range "+leftKey);
		logger.debug("Right range "+rightKey);
		this.rightKey = rightKey;
		this.leftKey = leftKey;
		leftKeyDatabaseEntry = currentSecondaryIndexKey = DatabaseEntryFactory.getInstance().createDatabaseEntry(leftKey);
		rightKeyDatabaseEntry = lastSecondaryIndexKey = DatabaseEntryFactory.getInstance().createDatabaseEntry(rightKey);
		this.column = column;
		binding = DatabaseEntryFactory.getInstance().databaseBinding(column.getType());
		
	}
	
	public void open() {
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(column.getDatabaseName());
		SecondaryDatabase database = BerkeleyDBEnvironment.getInstance().getIndexreference(column);
		tupleBinding = new RelationalTupleBinding(schema.getColumns());
		cursor = database.openCursor(null, null);
		currentPrimaryKey = new DatabaseEntry();
		currentPrimaryValue = new DatabaseEntry();
	}

	public Tuple next() {
		if (retVal == OperationStatus.NOTFOUND){
			retVal = cursor.getSearchKeyRange(currentSecondaryIndexKey, currentPrimaryKey, currentPrimaryValue, LockMode.DEFAULT);
			if (retVal==OperationStatus.SUCCESS){
				if (column.getType() == ColumnType.String){
					String currentSecondaryKey = (String)binding.entryToObject(currentSecondaryIndexKey);
					int compare = ((AtomicString)rightKey).getData().compareTo(currentSecondaryKey);
					if (compare<0)
						return null;
				}else
				if (column.getType() == ColumnType.Integer){
					Integer currentSecondaryKey = (Integer)binding.entryToObject(currentSecondaryIndexKey);
					int compare = ((AtomicInteger)rightKey).getData().compareTo(currentSecondaryKey);
					if (compare<0)
						return null;
				}
					
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
				if (column.getType() == ColumnType.String){
					String currentSecondaryKey = (String)binding.entryToObject(currentSecondaryIndexKey);
					int compare = ((AtomicString)rightKey).getData().compareTo(currentSecondaryKey);
					if (compare<0)
						return null;
				}else
					if (column.getType() == ColumnType.Integer){
						Integer currentSecondaryKey = (Integer)binding.entryToObject(currentSecondaryIndexKey);
						int compare = ((AtomicInteger)rightKey).getData().compareTo(currentSecondaryKey);
						if (compare<0)
							return null;
					}
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
