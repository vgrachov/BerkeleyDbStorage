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

import org.apache.log4j.Logger;
import org.brackit.relational.metadata.Schema;
import org.brackit.relational.metadata.tuple.AtomicChar;
import org.brackit.relational.metadata.tuple.AtomicDate;
import org.brackit.relational.metadata.tuple.AtomicDouble;
import org.brackit.relational.metadata.tuple.AtomicInteger;
import org.brackit.relational.metadata.tuple.AtomicString;
import org.brackit.relational.metadata.tuple.AtomicValue;
import org.brackit.relational.metadata.tuple.Column;
import org.brackit.relational.metadata.tuple.ColumnType;
import org.brackit.relational.metadata.tuple.Tuple;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.SecondaryDatabase;
import com.sleepycat.db.SecondaryKeyCreator;

public final class IndexValueCreator implements SecondaryKeyCreator {

	private static final Logger logger = Logger.getLogger(IndexValueCreator.class);
	
	private final RelationalTupleBinding tupleBinding;
	private final Column index;
	private final Schema schema;
	
	public IndexValueCreator(Schema schema,  RelationalTupleBinding tupleBinding, Column index){
		this.schema = schema;
		this.tupleBinding = tupleBinding;
		this.index = index;
	}
	
	public boolean createSecondaryKey(SecondaryDatabase secondary,
			DatabaseEntry key, DatabaseEntry data, DatabaseEntry result) {
		TupleInput keyInput = new TupleInput(key.getData());
		TupleInput dataInput = new TupleInput(data.getData());
		Tuple tuple = tupleBinding.smartEntryToObject(keyInput, dataInput);
		TupleOutput resultSerialized = new TupleOutput();
		AtomicValue[] fields = tuple.getFields();
		for (int i=0;i<schema.getColumns().length;i++){
			if (schema.getColumns()[i].getColumnName().equals(index.getColumnName())){
				if (index.getType() == ColumnType.String)
					resultSerialized.writeString(((AtomicString)fields[i]).getData());
				else
				if (index.getType() == ColumnType.Integer)
					resultSerialized.writeInt(((AtomicInteger)fields[i]).getData());
				else
				if (index.getType() == ColumnType.Double)
					resultSerialized.writeDouble(((AtomicDouble)fields[i]).getData());
				else
				if (index.getType() == ColumnType.Char)
					resultSerialized.writeChar(((AtomicChar)fields[i]).getData());
				else
				if (index.getType() == ColumnType.Date)
					resultSerialized.writeLong(((AtomicDate)fields[i]).getData());
				else
					throw new IllegalArgumentException("Can't create index for type "+index.getType());
			}
		}
		result.setData(resultSerialized.toByteArray());
		return true;
	}

}
