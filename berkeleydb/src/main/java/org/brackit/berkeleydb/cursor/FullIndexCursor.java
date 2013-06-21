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
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.relational.metadata.Schema;
import org.brackit.relational.metadata.tuple.Column;
import org.brackit.relational.metadata.tuple.Tuple;

import com.sleepycat.db.CursorConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.SecondaryCursor;
import com.sleepycat.db.Transaction;

public class FullIndexCursor extends AbstractCursor {

	private static final Logger logger = Logger.getLogger(FullIndexCursor.class);
	
	private Column column;
	private SecondaryCursor cursor;
	private DatabaseEntry secondaryKey = new DatabaseEntry();
	private DatabaseEntry primaryKey = new DatabaseEntry();
	private DatabaseEntry primaryValue = new DatabaseEntry();
	private OperationStatus retVal = OperationStatus.NOTFOUND;
	
	FullIndexCursor(Schema schema, Column column, Transaction transaction, Set<String> projectionFields){
		super(schema, projectionFields, transaction);
		this.column = column;
	}
	
	public void open() {
		try {
			cursor = BerkeleyDBEnvironment.getInstance().getIndexreference(column).openSecondaryCursor(transaction, CursorConfig.DEFAULT);
		} catch (DatabaseException e) {
			logger.info(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	public Tuple next() {
		if (retVal == OperationStatus.NOTFOUND) {
			try {
				retVal = cursor.getNext(secondaryKey, primaryKey, primaryValue, LockMode.DEFAULT);
			} catch (DatabaseException e) {
				logger.info(e.getMessage());
				throw new RuntimeException(e);
			}
			if (retVal != OperationStatus.NOTFOUND) {
				return getReconstractedTuple(primaryKey, primaryValue);
			}else
				return null;
		}else{
			try {
				retVal = cursor.getNextDup(secondaryKey, primaryKey, primaryValue, LockMode.DEFAULT);
			} catch (DatabaseException e) {
				logger.info(e.getMessage());
				throw new RuntimeException(e);
			}
			if (retVal != OperationStatus.NOTFOUND) {
				return getReconstractedTuple(primaryKey, primaryValue);
			}else{
				try {
					retVal = cursor.getNext(secondaryKey, primaryKey, primaryValue, LockMode.DEFAULT);
				} catch (DatabaseException e) {
					logger.info(e.getMessage());
					throw new RuntimeException(e);
				}
				if (retVal != OperationStatus.NOTFOUND) {
					return getReconstractedTuple(primaryKey, primaryValue);
				}else
					return null;
			}
		}
	}

	public void close() {
		try {
			cursor.close();
		} catch (DatabaseException e) {
			logger.info(e.getMessage());
			throw new RuntimeException(e);
		}
	}
}
