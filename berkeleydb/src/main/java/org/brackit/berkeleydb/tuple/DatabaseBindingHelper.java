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

import java.util.Date;

import org.brackit.berkeleydb.binding.typebinding.DateBinding;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;

public final class DatabaseBindingHelper {
	
	private DateBinding dateBinding = new DateBinding();

	private static class DatabaseBindingHelperHolder{
		private static DatabaseBindingHelper instance = new DatabaseBindingHelper(); 
	}
	
	private DatabaseBindingHelper(){
		
	}
	
	public static DatabaseBindingHelper getInstance(){
		return DatabaseBindingHelperHolder.instance;
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
		if (columnType == ColumnType.Char)
			return TupleBinding.getPrimitiveBinding(Character.class);
		else
		if (columnType == ColumnType.Date)
			return dateBinding;
		else
			throw new IllegalArgumentException("Column type has not corresponding binding");
	}

}
