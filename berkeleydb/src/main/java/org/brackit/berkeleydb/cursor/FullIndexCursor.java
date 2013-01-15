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

import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.relational.api.cursor.ITupleCursor;
import org.brackit.relational.metadata.Schema;
import org.brackit.relational.metadata.tuple.Column;
import org.brackit.relational.metadata.tuple.Tuple;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.SecondaryCursor;
import com.sleepycat.db.Transaction;

public class FullIndexCursor implements ITupleCursor {

	private Column column;
	private SecondaryCursor cursor;
	private DatabaseEntry secondaryKey = new DatabaseEntry();
	private DatabaseEntry primaryKey = new DatabaseEntry();
	private DatabaseEntry primaryValue = new DatabaseEntry();
	private RelationalTupleBinding tupleBinding;
	private OperationStatus retVal = OperationStatus.NOTFOUND;
	private final Transaction transaction;
	
	FullIndexCursor(Column column, Transaction transaction){
		this.column = column;
		this.transaction = transaction;
	}
	
	public void open() {
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(column.getDatabaseName());
		try {
			cursor = BerkeleyDBEnvironment.getInstance().getIndexreference(column).openSecondaryCursor(transaction, null);
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
		tupleBinding = new RelationalTupleBinding(schema.getColumns());
	}

	public Tuple next() {
		if (retVal == OperationStatus.NOTFOUND){
			try {
				retVal = cursor.getNext(secondaryKey, primaryKey, primaryValue, LockMode.DEFAULT);
			} catch (DatabaseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (retVal!=OperationStatus.NOTFOUND){
				TupleInput foundKeySerialized = new TupleInput(primaryKey.getData());
				TupleInput foundDataSerialized = new TupleInput(primaryValue.getData());
				return tupleBinding.smartEntryToObject(foundKeySerialized, foundDataSerialized);
			}else
				return null;
		}else{
			try {
				retVal = cursor.getNextDup(secondaryKey, primaryKey, primaryValue, LockMode.DEFAULT);
			} catch (DatabaseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (retVal!=OperationStatus.NOTFOUND){
				TupleInput foundKeySerialized = new TupleInput(primaryKey.getData());
				TupleInput foundDataSerialized = new TupleInput(primaryValue.getData());
				return tupleBinding.smartEntryToObject(foundKeySerialized, foundDataSerialized);
			}else{
				try {
					retVal = cursor.getNext(secondaryKey, primaryKey, primaryValue, LockMode.DEFAULT);
				} catch (DatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
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
		try {
			cursor.close();
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
