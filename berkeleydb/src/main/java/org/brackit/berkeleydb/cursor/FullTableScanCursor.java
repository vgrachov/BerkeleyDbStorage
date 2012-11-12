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

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.DatabaseAccess;
import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.tuple.Tuple;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class FullTableScanCursor extends DatabaseAccess implements ITupleCursor {

	private static final Logger logger = Logger.getLogger(FullTableScanCursor.class);
	
	private DiskOrderedCursor cursor;
	
	private final RelationalTupleBinding tupleBinding;
	
	
	public FullTableScanCursor(String databaseName){
		super(databaseName);
		tupleBinding = new RelationalTupleBinding(schema.getColumns());
	}
	
	public void open() {
		logger.debug("Open cursor for database "+super.dataBase.getDatabaseName());
		cursor = dataBase.openCursor(DiskOrderedCursorConfig.DEFAULT);
	}

	public Tuple next() {
		DatabaseEntry elementKey = new DatabaseEntry();
		DatabaseEntry elementData = new DatabaseEntry();
		OperationStatus status = cursor.getNext(elementKey, elementData, LockMode.READ_UNCOMMITTED);
		if (status == OperationStatus.SUCCESS){
			Tuple tuple = tupleBinding.smartEntryToObject(new TupleInput(elementKey.getData()), new TupleInput(elementData.getData()));
			return tuple;
		}
		return null;
	}

	public void close() {
		cursor.close();
	}

}
