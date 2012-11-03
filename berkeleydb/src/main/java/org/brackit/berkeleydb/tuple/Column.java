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

public class Column {

	private final String columnName;
	
	private final ColumnType type;
	
	private final boolean isBelongToKey;
	
	private final boolean isDirectIndexExist;
	
	private final String databaseName;

	
	public Column(String databaseName, String columnName, ColumnType type, boolean isBelongToKey, boolean isDirectIndexExist) {
		this.databaseName = databaseName;
		this.columnName = columnName;
		this.type = type;
		this.isBelongToKey = isBelongToKey;
		this.isDirectIndexExist = isDirectIndexExist;
	}

	public String getColumnName() {
		return columnName;
	}

	public ColumnType getType() {
		return type;
	}

	@Override
	public String toString() {
		return columnName+" - "+type+" - "+isBelongToKey+" - "+isDirectIndexExist;
	}

	public boolean isBelongToKey() {
		return isBelongToKey;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public boolean isDirectIndexExist() {
		return isDirectIndexExist;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((columnName == null) ? 0 : columnName.hashCode());
		result = prime * result
				+ ((databaseName == null) ? 0 : databaseName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Column other = (Column) obj;
		if (columnName == null) {
			if (other.columnName != null)
				return false;
		} else if (!columnName.equals(other.columnName))
			return false;
		if (databaseName == null) {
			if (other.databaseName != null)
				return false;
		} else if (!databaseName.equals(other.databaseName))
			return false;
		return true;
	}

	
}
