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
package org.brackit.berkeleydb.cursor;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.tuple.Atomic;
import org.brackit.berkeleydb.tuple.Column;


public final class TupleCursorFactory {
	
	private static final Logger logger = Logger.getLogger(TupleCursorFactory.class);
	
	private static class TupleCursorFactoryHolder{
		private static TupleCursorFactory instance = new TupleCursorFactory();
	}
	
	
	public static TupleCursorFactory getInstance(){
		return TupleCursorFactoryHolder.instance;
	}
	
	public TupleCursor equalMatchingQuery(Column column, Atomic value){
		return null;
	}
	
}
