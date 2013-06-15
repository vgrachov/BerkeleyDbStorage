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

import java.util.Set;

import org.apache.log4j.Logger;
import org.brackit.relational.metadata.tuple.AtomicChar;
import org.brackit.relational.metadata.tuple.AtomicDate;
import org.brackit.relational.metadata.tuple.AtomicDouble;
import org.brackit.relational.metadata.tuple.AtomicInteger;
import org.brackit.relational.metadata.tuple.AtomicString;
import org.brackit.relational.metadata.tuple.AtomicValue;
import org.brackit.relational.metadata.tuple.Column;
import org.brackit.relational.metadata.tuple.ColumnType;
import org.brackit.relational.metadata.tuple.Tuple;

import com.google.common.base.Preconditions;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class RelationalTupleBinding {

	private static final Logger logger = Logger.getLogger(RelationalTupleBinding.class);
	
	private final Column[] schema;
	
	public RelationalTupleBinding(Column[] schema){
		logger.debug("RDBMSTupleBinding is created");
		this.schema = schema.clone();
	}

	public Tuple smartEntryToObject(TupleInput key, TupleInput input, Set<String> projectionFields){
		Preconditions.checkNotNull(projectionFields);
		AtomicValue[] fields = new AtomicValue[projectionFields.size()];
		int columnIndex = 0;
		for (int i=0;i<schema.length;i++){
			
			TupleInput target = null;
			if (schema[i].isBelongToKey())
				target = key;
			else
				target = input;

			Column currentColumn = schema[i];
			if (currentColumn.getType()==ColumnType.String) {
				String data = target.readString();
				if (projectionFields.contains(currentColumn.getColumnName()))
					fields[columnIndex++] = new AtomicString(currentColumn.getColumnName(), data);
			} else
			if (currentColumn.getType()==ColumnType.Integer) {
				int data = target.readInt();
				if (projectionFields.contains(currentColumn.getColumnName()))
					fields[columnIndex++] = new AtomicInteger(currentColumn.getColumnName(), data);
			} else
			if (currentColumn.getType()==ColumnType.Double) {
				double data = target.readDouble();
				if (projectionFields.contains(currentColumn.getColumnName()))
					fields[columnIndex++] = new AtomicDouble(currentColumn.getColumnName(), data);
			} else
			if (currentColumn.getType()==ColumnType.Char) {
				char data = target.readChar();
				if (projectionFields.contains(currentColumn.getColumnName()))
					fields[columnIndex++] = new AtomicChar(currentColumn.getColumnName(), data);
			} else
			if (currentColumn.getType()==ColumnType.Date) {
				long data = target.readLong();
				if (projectionFields.contains(currentColumn.getColumnName()))
					fields[columnIndex++] = new AtomicDate(currentColumn.getColumnName(), data);
			} else
				throw new IllegalArgumentException("Type is not supported");
		}
		return new Tuple(fields);	
	}
	
	public void smartObjectToEntry(Tuple tuple, TupleOutput key, TupleOutput output){
		AtomicValue[] fields = tuple.getFields();
		for (int i=0;i<schema.length;i++){
			TupleOutput target = null;
			if (schema[i].isBelongToKey())
				target = key;
			else
				target = output;
			if (schema[i].getType() == ColumnType.String)
				target.writeString( (String)fields[i].getData() );
			else
			if (schema[i].getType() == ColumnType.Integer)
				target.writeInt((Integer)fields[i].getData() );
			else
			if (schema[i].getType() == ColumnType.Double)
				target.writeDouble ((Double)fields[i].getData() );
			else
			if (schema[i].getType() == ColumnType.Char)
				target.writeChar ((Character)fields[i].getData() );
			else
			if (schema[i].getType() == ColumnType.Date)
				target.writeLong((Long)fields[i].getData());
		}
	}
}
