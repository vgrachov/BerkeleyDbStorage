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
package org.brackit.berkeleydb.comparator;

import java.io.Serializable;
import java.util.*;

import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.tuple.Atomic;
import org.brackit.berkeleydb.tuple.AtomicDouble;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;
import org.brackit.berkeleydb.tuple.Tuple;

import com.sleepycat.je.DatabaseEntry;

public class TupleComparator implements Comparator<byte[]>, Serializable {

	private Column[] columns;
	
	public TupleComparator(){
		
	}
	
	public TupleComparator(Column[] columns){
		this.columns = columns;
	}

	public int compare(byte[] firstTuple, byte[] secondTuple) {
		RelationalTupleBinding tupleBinding = new RelationalTupleBinding(columns);
		DatabaseEntry firstEntry = new DatabaseEntry(firstTuple);
		DatabaseEntry secondEntry = new DatabaseEntry(secondTuple);
		Tuple first = tupleBinding.entryToObject(firstEntry);
		Tuple second = tupleBinding.entryToObject(secondEntry);
		for (int i=0;i<columns.length;i++){
			if (columns[i].isBelongToKey()){
				int compareResult = 0 ;
				if (columns[i].getType()==ColumnType.String){
					AtomicString firtsField = (AtomicString) first.getFields()[i];
					AtomicString secondField = (AtomicString) second.getFields()[i];
					compareResult = firtsField.compareTo(secondField);
				}else
				if (columns[i].getType()==ColumnType.Integer){
					AtomicInteger firtsField = (AtomicInteger) first.getFields()[i];
					AtomicInteger secondField = (AtomicInteger) second.getFields()[i];
					compareResult = firtsField.compareTo(secondField);
				}else
				if (columns[i].getType()==ColumnType.Double){
					AtomicDouble firtsField = (AtomicDouble) first.getFields()[i];
					AtomicDouble secondField = (AtomicDouble) second.getFields()[i];
					compareResult = firtsField.compareTo(secondField);
				}
				if (compareResult!=0)
					return compareResult;
			}
		}
		return 0;
	}

}
