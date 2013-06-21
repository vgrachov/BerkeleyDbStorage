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

import org.brackit.berkeleydb.mapping.TupleMapper;
import org.brackit.relational.api.cursor.ITupleCursor;
import org.brackit.relational.metadata.Schema;
import org.brackit.relational.metadata.tuple.AtomicValue;
import org.brackit.relational.metadata.tuple.Tuple;

import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.Transaction;

/**
 * Provides facility for reconstruction Tuple from key-value pairs
 */
public abstract class AbstractCursor implements ITupleCursor{

	protected final Set<String> projectionFields;
	protected final Schema schema;
	protected final TupleMapper tupleMapper;
	protected final Transaction transaction;
	
	protected AbstractCursor(Schema schema, Set<String> projectionFields, Transaction transaction) {
		this.projectionFields = projectionFields;
		this.schema = schema;
		this.transaction = transaction;
		this.tupleMapper = new TupleMapper(schema);
	}
	
	protected Tuple getReconstractedTuple(DatabaseEntry key, DatabaseEntry value) {
		AtomicValue[] fields = new AtomicValue[projectionFields.size()];
		int index = 0;
		for (int i=0; i<schema.getColumns().length; i++) {
			if (projectionFields.contains(schema.getColumns()[i].getColumnName())) {
				fields[index++] = tupleMapper.getValueByFieldName(schema.getColumns()[i].getColumnName(), key.getData(), value.getData());
			}
		}
		return new Tuple(fields);
	}
}
