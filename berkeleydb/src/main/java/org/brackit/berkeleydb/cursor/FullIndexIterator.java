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

import org.brackit.berkeleydb.Catalog;
import org.brackit.berkeleydb.Schema;
import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.impl.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.Tuple;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;

public class FullIndexIterator implements ITupleCursor {

	private Column column;
	private SecondaryCursor cursor;
	private DatabaseEntry secondaryKey = new DatabaseEntry();
	private DatabaseEntry primaryKey = new DatabaseEntry();
	private DatabaseEntry primaryValue = new DatabaseEntry();
	private RelationalTupleBinding tupleBinding;
	private OperationStatus retVal = OperationStatus.NOTFOUND;
	
	public FullIndexIterator(Column column){
		this.column = column;
	}
	
	public void open() {
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(column.getDatabaseName());
		cursor = BerkeleyDBEnvironment.getInstance().getIndexreference(column).openCursor(null, null);
		tupleBinding = new RelationalTupleBinding(schema.getColumns());
	}

	public Tuple next() {
		if (retVal == OperationStatus.NOTFOUND){
			retVal = cursor.getNext(secondaryKey, primaryKey, primaryValue, LockMode.DEFAULT);
			if (retVal!=OperationStatus.NOTFOUND){
				TupleInput foundKeySerialized = new TupleInput(primaryKey.getData());
				TupleInput foundDataSerialized = new TupleInput(primaryValue.getData());
				return tupleBinding.smartEntryToObject(foundKeySerialized, foundDataSerialized);
			}else
				return null;
		}else{
			retVal = cursor.getNextDup(secondaryKey, primaryKey, primaryValue, LockMode.DEFAULT);
			if (retVal!=OperationStatus.NOTFOUND){
				TupleInput foundKeySerialized = new TupleInput(primaryKey.getData());
				TupleInput foundDataSerialized = new TupleInput(primaryValue.getData());
				return tupleBinding.smartEntryToObject(foundKeySerialized, foundDataSerialized);
			}else{
				retVal = cursor.getNext(secondaryKey, primaryKey, primaryValue, LockMode.DEFAULT);
				if (retVal!=OperationStatus.NOTFOUND){
					TupleInput foundKeySerialized = new TupleInput(primaryKey.getData());
					TupleInput foundDataSerialized = new TupleInput(primaryValue.getData());
					return tupleBinding.smartEntryToObject(foundKeySerialized, foundDataSerialized);
				}else
					return null;
			}
			
		}
	}

	public void close() {
		cursor.close();
		
	}

}
