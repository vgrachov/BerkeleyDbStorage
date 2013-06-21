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
package org.brackit.berkeleydb.binding;

import org.brackit.berkeleydb.mapping.TupleMapper;
import org.brackit.relational.metadata.Schema;
import org.brackit.relational.metadata.tuple.AtomicChar;
import org.brackit.relational.metadata.tuple.AtomicDate;
import org.brackit.relational.metadata.tuple.AtomicDouble;
import org.brackit.relational.metadata.tuple.AtomicInteger;
import org.brackit.relational.metadata.tuple.AtomicString;
import org.brackit.relational.metadata.tuple.AtomicValue;
import org.brackit.relational.metadata.tuple.Column;
import org.brackit.relational.metadata.tuple.ColumnType;

import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.SecondaryDatabase;
import com.sleepycat.db.SecondaryKeyCreator;

public final class IndexValueCreator implements SecondaryKeyCreator {
	
	private final Column index;
	private final TupleMapper tupleMapper;
	
	public IndexValueCreator(Schema schema, Column index){
		this.index = index;
		this.tupleMapper = new TupleMapper(schema);
	}
	
	public boolean createSecondaryKey(SecondaryDatabase secondary,
			DatabaseEntry key, DatabaseEntry value, DatabaseEntry result) {
		AtomicValue fieldValue = tupleMapper.getValueByFieldName(index.getColumnName(), key.getData(), value.getData());
		TupleOutput resultSerialized = new TupleOutput();
		
		if (index.getType() == ColumnType.String)
			resultSerialized.writeString(((AtomicString)fieldValue).getData());
		else
		if (index.getType() == ColumnType.Integer)
			resultSerialized.writeInt(((AtomicInteger)fieldValue).getData());
		else
		if (index.getType() == ColumnType.Double)
			resultSerialized.writeDouble(((AtomicDouble)fieldValue).getData());
		else
		if (index.getType() == ColumnType.Char)
			resultSerialized.writeChar(((AtomicChar)fieldValue).getData());
		else
		if (index.getType() == ColumnType.Date)
			resultSerialized.writeLong(((AtomicDate)fieldValue).getData());
		else
			throw new IllegalArgumentException("Can't create index for type "+index.getType());

		result.setData(resultSerialized.toByteArray());
		return true;
	}
}
