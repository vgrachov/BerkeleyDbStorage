/*******************************************************************************
 * [New BSD License]
 *  Copyright (c) 2012, Volodymyr Grachov <vladimir.grachov@gmail.com>  
 *  All rights reserved.
 *  
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *      * Redistributions of source code must retain the above copyright
 *        notice, this list of conditions and the following disclaimer.
 *      * Redistributions in binary form must reproduce the above copyright
 *        notice, this list of conditions and the following disclaimer in the
 *        documentation and/or other materials provided with the distribution.
 *      * Neither the name of the Brackit Project Team nor the
 *        names of its contributors may be used to endorse or promote products
 *        derived from this software without specific prior written permission.
 *  
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 *  ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.SecondaryCursor;
import com.sleepycat.db.Transaction;

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
	private final Transaction transaction;

	public EqualMatchIndexSearchCursor(String databaseName, Column column, Atomic value){
		this(databaseName,column,value,null);
	}
	
	public EqualMatchIndexSearchCursor(String databaseName, Column column, Atomic value, Transaction transaction){
		this.databaseName = databaseName;
		this.column = column;
		this.searchValue = value;
		this.transaction = transaction;
	}
	
	public void open() {
		logger.debug("Open cursor over database "+databaseName+" for column "+column.getColumnName());
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(databaseName);
		try {
			cursor=BerkeleyDBEnvironment.getInstance().getIndexreference(column).openSecondaryCursor(transaction, null);
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		//TODO: review move to next operation
		try {
			retVal = cursor.getSearchKey(searchKeyEntry, foundKeyEntry, foundDataEntry, LockMode.DEFAULT);
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Tuple next() {
		if (retVal == OperationStatus.SUCCESS){
			TupleInput foundKeySerialized = new TupleInput(foundKeyEntry.getData());
			TupleInput foundDataSerialized = new TupleInput(foundDataEntry.getData());
			Tuple tuple = tupleBinding.smartEntryToObject(foundKeySerialized, foundDataSerialized);
			try {
				retVal = cursor.getNextDup(searchKeyEntry, foundKeyEntry, foundDataEntry, LockMode.DEFAULT);
			} catch (DatabaseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return tuple;
		}else
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
