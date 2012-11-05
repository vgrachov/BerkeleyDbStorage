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

public class AtomicDate extends Atomic<Date> implements Comparable<AtomicDate> {

	private Date data;

	public AtomicDate(String fieldName, long data){
		super(fieldName);
		this.data = new Date(data);
	}
	
	public AtomicDate(String fieldName, Date data){
		super(fieldName);
		this.data = data;
	}

	@Override
	public Date getData() {
		return data;
	}
	
	@Override
	public String toString() {
		return data.toString();
	}

	public int compareTo(AtomicDate o) {
		return data.compareTo(o.getData());
	}

}
