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
import org.brackit.berkeleydb.Schema;
import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.Atomic;
import org.brackit.berkeleydb.tuple.AtomicDouble;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.Tuple;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;

public class EqualMatchIndexSearchCursor implements ITupleCursor {

	private static final Logger logger = Logger.getLogger(EqualMatchIndexSearchCursor.class);

	private SecondaryCursor cursor;
	
	private final Atomic searchValue;
	
	private final Column column;
	
	private RelationalTupleBinding tupleBinding;
	
	private DatabaseEntry searchKeyEntry;
	private DatabaseEntry foundKeyEntry;
	private DatabaseEntry foundDataEntry;
	
	private String databaseName;
	private OperationStatus retVal;
	
	public EqualMatchIndexSearchCursor(String databaseName, Column column, Atomic value){
		this.databaseName = databaseName;
		this.column = column;
		this.searchValue = value;
	}
	
	public void open() {
		logger.debug("Open cursor over database "+databaseName+" for column "+column.getColumnName());
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(databaseName);
		cursor=BerkeleyDBEnvironment.getInstance().getIndexreference(column).openCursor(null, null);
		tupleBinding = new RelationalTupleBinding(schema.getColumns());
		
		TupleOutput searchKeySerialized = new TupleOutput();
		if (searchValue instanceof AtomicString)
			searchKeySerialized.writeString(((AtomicString)searchValue).getData());
		else
		if (searchValue instanceof AtomicInteger)
			searchKeySerialized.writeInt(((AtomicInteger)searchValue).getData());
		else
		if (searchValue instanceof AtomicDouble)
			searchKeySerialized.writeDouble(((AtomicDouble)searchValue).getData());
		searchKeyEntry = new DatabaseEntry(searchKeySerialized.toByteArray()); 
		
		foundKeyEntry = new DatabaseEntry();
		foundDataEntry = new DatabaseEntry();
		
		retVal = cursor.getSearchKey(searchKeyEntry, foundKeyEntry, foundDataEntry, LockMode.DEFAULT);
	}

	public Tuple next() {
		if (retVal == OperationStatus.SUCCESS){
			TupleInput foundKeySerialized = new TupleInput(foundKeyEntry.getData());
			TupleInput foundDataSerialized = new TupleInput(foundDataEntry.getData());
			Tuple tuple = tupleBinding.smartEntryToObject(foundKeySerialized, foundDataSerialized);
			retVal = cursor.getNextDup(searchKeyEntry, foundKeyEntry, foundDataEntry, LockMode.DEFAULT);
			return tuple;
		}else
			return null;
	}

	public void close() {
		cursor.close();
	}

}
