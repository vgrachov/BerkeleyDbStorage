/*******************************************************************************
 * [New BSD License]
 *  Copyright (c) 2012, Volodymyr Grachov <vladimir.grachov@gmail.com>  
 *  All rights reserved.
 *  
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *      * Redistributions of source code must retain the above copyright
 *        notice, this list of conditions and the following disclaimer.
 *      * Redistributions in binary form must reproduce the above copyright
 *        notice, this list of conditions and the following disclaimer in the
 *        documentation and/or other materials provided with the distribution.
 *      * Neither the name of the Brackit Project Team nor the
 *        names of its contributors may be used to endorse or promote products
 *        derived from this software without specific prior written permission.
 *  
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 *  ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package org.brackit.berkeleydb.tuple;

public final class Column {

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
