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
package org.brackit.berkeleydb.binding;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.tuple.Atomic;
import org.brackit.berkeleydb.tuple.AtomicDouble;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;
import org.brackit.berkeleydb.tuple.Tuple;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class RelationalTupleBinding extends TupleBinding<Tuple> {

	private static final Logger logger = Logger.getLogger(RelationalTupleBinding.class);
	
	private final Column[] schema;
	
	public RelationalTupleBinding(Column[] schema){
		logger.debug("RDBMSTupleBinding is created");
		this.schema = schema.clone();
	}

	public Tuple smartEntryToObject(TupleInput key, TupleInput input){
		Atomic[] fields = new Atomic[schema.length];
		for (int i=0;i<schema.length;i++){
			
			TupleInput target = null;
			if (schema[i].isBelongToKey())
				target = key;
			else
				target = input;
			
			Column currentColumn = schema[i];
			if (currentColumn.getType()==ColumnType.String)
				fields[i] = new AtomicString(currentColumn.getColumnName(), target.readString());
			else
			if (currentColumn.getType()==ColumnType.Integer)
				fields[i] = new AtomicInteger(currentColumn.getColumnName(), target.readInt());
			else
			if (currentColumn.getType()==ColumnType.Double)
				fields[i] = new AtomicDouble(currentColumn.getColumnName(), target.readDouble());
		}
		return new Tuple(fields);
		
	}
	
	@Override
	@Deprecated
	public Tuple entryToObject(TupleInput input) {
		throw new IllegalAccessError();
	}

	@Override
	@Deprecated
	public void objectToEntry(Tuple tuple, TupleOutput output) {
		throw new IllegalAccessError();
	}

	public void smartObjectToEntry(Tuple tuple, TupleOutput key, TupleOutput output){
		Atomic[] fields = tuple.getFields();
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
		}
	}
}
