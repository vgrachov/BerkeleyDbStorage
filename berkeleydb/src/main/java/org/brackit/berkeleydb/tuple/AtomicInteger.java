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

public class AtomicInteger extends Atomic<Integer> implements Comparable<AtomicInteger> {

	private Integer data;
	
	public AtomicInteger(String fieldName,Integer data){
		super(fieldName);
		this.data = data;
	}

	public Integer getData() {
		return data;
	}

	@Override
	public String toString() {
		return data.toString();
	}

	public int compareTo(AtomicInteger o) {
		return data.compareTo(o.getData());
	}
}
