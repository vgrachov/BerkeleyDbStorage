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

import org.brackit.berkeleydb.Schema;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class CatalogTupleBinding extends TupleBinding<Schema> {

	@Override
	public Schema entryToObject(TupleInput input) {
		String databaseName = input.readString();
		Integer columnNumber = input.readInt();
		Column[] columns = new Column[columnNumber];
		for (int i=0;i<columnNumber;i++){
			String columnName = input.readString();
			ColumnType columnType = ColumnType.valueOf(input.readString());
			Boolean isBelongToKey = input.readBoolean();
			Boolean isDirectIndexExist = input.readBoolean();
			columns[i] = new Column(databaseName,columnName, columnType, isBelongToKey, isDirectIndexExist);
		}
		return new Schema(columns, databaseName);
	}

	@Override
	public void objectToEntry(Schema schema, TupleOutput output) {
		output.writeString(schema.getDatabaseName());
		output.writeInt(schema.getColumns().length);
		for (int i=0;i<schema.getColumns().length;i++){
			output.writeString(schema.getColumns()[i].getColumnName());
			output.writeString(schema.getColumns()[i].getType().toString());
			output.writeBoolean(schema.getColumns()[i].isBelongToKey());
			output.writeBoolean(schema.getColumns()[i].isDirectIndexExist());
		}
	}

}
