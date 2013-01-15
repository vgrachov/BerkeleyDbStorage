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

import org.brackit.relational.metadata.Schema;
import org.brackit.relational.metadata.tuple.Column;
import org.brackit.relational.metadata.tuple.ColumnType;

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
