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
package org.brackit.berkeleydb.tuple;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;

public final class DatabaseEntryFactory {
	
	private static class DatabaseEntryFactoryHolder{
		private static DatabaseEntryFactory instance = new DatabaseEntryFactory(); 
	}
	
	public DatabaseEntryFactory(){
		
	}
	
	public static DatabaseEntryFactory getInstance(){
		return DatabaseEntryFactoryHolder.instance;
	}
	
	public final DatabaseEntry createDatabaseEntry(Atomic value){
		TupleOutput output = new TupleOutput();
		if (value instanceof AtomicString)
			output.writeString(((AtomicString)value).getData());
		else
		if (value instanceof AtomicInteger)
			output.writeInt(((AtomicInteger)value).getData());
		else
		if (value instanceof AtomicDouble)
			output.writeDouble(((AtomicDouble)value).getData());
		DatabaseEntry databaseEntry = new DatabaseEntry(output.toByteArray());
		return databaseEntry;
	}

	public final EntryBinding databaseBinding(ColumnType columnType){
		if (columnType == ColumnType.String)
			return TupleBinding.getPrimitiveBinding(String.class);
		else
		if (columnType == ColumnType.Integer)
			return TupleBinding.getPrimitiveBinding(Integer.class);
		else
		if (columnType == ColumnType.Double)
			return TupleBinding.getPrimitiveBinding(Double.class);
		return null;
	}
	
}
