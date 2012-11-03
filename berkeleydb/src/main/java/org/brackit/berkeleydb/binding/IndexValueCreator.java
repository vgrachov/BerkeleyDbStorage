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

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.tuple.Atomic;
import org.brackit.berkeleydb.tuple.AtomicDouble;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;
import org.brackit.berkeleydb.tuple.Tuple;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

public final class IndexValueCreator implements SecondaryKeyCreator {

	private static final Logger logger = Logger.getLogger(IndexValueCreator.class);
	
	private final RelationalTupleBinding tupleBinding;
	private final Column index;
	
	public IndexValueCreator(RelationalTupleBinding tupleBinding, Column index){
		this.tupleBinding = tupleBinding;
		this.index = index;
	}
	
	public boolean createSecondaryKey(SecondaryDatabase secondary,
			DatabaseEntry key, DatabaseEntry data, DatabaseEntry result) {
		logger.debug("Create index for "+index.getColumnName());
		TupleInput keyInput = new TupleInput(key.getData());
		TupleInput dataInput = new TupleInput(data.getData());
		Tuple tuple = tupleBinding.smartEntryToObject(keyInput, dataInput);
		TupleOutput resultSerialized = new TupleOutput();
		Atomic[] fields = tuple.getFields();
		for (int i=0;i<fields.length;i++){
			if (fields[i].getFieldName().equals(index.getColumnName())){
				logger.debug("Column matching is find");
				if (index.getType() == ColumnType.String)
					resultSerialized.writeString(((AtomicString)fields[i]).getData());
				else
				if (index.getType() == ColumnType.Integer)
					resultSerialized.writeInt(((AtomicInteger)fields[i]).getData());
				else
				if (index.getType() == ColumnType.Double)
					resultSerialized.writeDouble(((AtomicDouble)fields[i]).getData());
				else
					throw new IllegalArgumentException();
			}
		}
		result.setData(resultSerialized.toByteArray());
		return true;
	}

}
