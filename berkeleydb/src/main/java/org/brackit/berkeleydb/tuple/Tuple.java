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

import java.io.Serializable;

public class Tuple implements Serializable {

	private Atomic[] fields;

	public Atomic[] getFields() {
		return fields;
	}

	public void setFields(Atomic[] fields) {
		this.fields = fields.clone();
	}

	public Tuple(Atomic[] fields) {
		this.fields = fields.clone();
	}

	@Override
	public String toString(){
		StringBuilder builder = new StringBuilder();
		for (int i=0;i<fields.length;i++){
			if (i!=0)
				builder.append(",");
			builder.append(fields[i]);
		}
		return builder.toString();
	}
	
}
