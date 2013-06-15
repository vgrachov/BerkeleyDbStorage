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

import java.util.Set;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.relational.api.cursor.Condition;
import org.brackit.relational.api.cursor.ITupleCursor;
import org.brackit.relational.metadata.tuple.AtomicValue;
import org.brackit.relational.metadata.tuple.Tuple;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.CursorConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.Transaction;

public class FullTableScanCursor extends BerkeleydbDatabaseAccess implements ITupleCursor {

	private static final Logger logger = Logger.getLogger(FullTableScanCursor.class);
	
	private Cursor cursor;
	
	private final RelationalTupleBinding tupleBinding;
	
	private final String databaseName;
	
	private final Transaction transaction;
	
	private final Condition condition;
	
	private int conditionPosition;
	
	private final Set<String> projectionFields;
	
	public FullTableScanCursor(String databaseName, Transaction transaction, Set<String> projectionFields){
		this(databaseName, transaction, null, projectionFields);
		/*super(databaseName);
		this.databaseName = databaseName;
		tupleBinding = new RelationalTupleBinding(schema.getColumns());
		this.transaction = transaction;
		this.condition = null;*/
	}

	public FullTableScanCursor(String databaseName, Transaction transaction, Condition condition, Set<String> projectionFields){
		super(databaseName);
		this.databaseName = databaseName;
		tupleBinding = new RelationalTupleBinding(schema.getColumns());
		this.transaction = transaction;
		this.condition = condition;
		this.projectionFields = projectionFields;
	}

	public void open() {
		try {
			/*for (int i=0; i < schema.getColumns().length; i++)
				if (condition.getColumn().equals(schema.getColumns()[i]))
					conditionPosition = i;
			conditionPosition = -1;*/
			cursor = dataBase.openCursor(transaction, CursorConfig.DEFAULT);
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public Tuple next() {
		DatabaseEntry elementKey = new DatabaseEntry();
		DatabaseEntry elementData = new DatabaseEntry();
		OperationStatus status = null;
		//while (true) {
			try {
				status = cursor.getNext(elementKey, elementData, LockMode.DEFAULT);
			} catch (DatabaseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (status == OperationStatus.SUCCESS) {
				Tuple tuple = tupleBinding.smartEntryToObject(new TupleInput(elementKey.getData()), new TupleInput(elementData.getData()), projectionFields);
				return tuple;
/*				if (condition == null)
					return tuple;
				else {
					AtomicValue currentValue = tuple.getFields()[conditionPosition];
					if (isConditionFulfilled(currentValue))
						return tuple;
				}*/
			}// else
			//	break;
		//}
		return null;
	}

	private boolean isConditionFulfilled(AtomicValue value) {
		if (condition == null)
			throw new IllegalArgumentException("Condition can't be null");
		if (value == null)
			throw new IllegalArgumentException("Value can't be null");
		value.getData();
		return true;
		//if (condition.getSign() == Condition.Sign.)
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
