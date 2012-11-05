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
package org.brackit.berkeleydb;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.catalog.ICatalog;
import org.brackit.berkeleydb.exception.KeyDuplicationException;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;
import org.junit.Assert;
import org.junit.Test;

public class USER_DB_CREATE {

	private static final Logger logger = Logger.getLogger(USER_DB_CREATE.class);
	
	@Test
	public void createDatabase(){
		ICatalog catalog = Catalog.getInstance();
		Schema schema = new Schema(new Column[]{
				new Column("USER_TABLE", "ID", ColumnType.Integer, true, true),
				new Column("USER_TABLE", "Login", ColumnType.String, false, true),
				new Column("USER_TABLE", "Password", ColumnType.String, false, false),
				new Column("USER_TABLE", "Salary", ColumnType.Integer, false, true),
				new Column("USER_TABLE", "Test", ColumnType.Integer, false, true),
				new Column("USER_TABLE", "Birthday", ColumnType.String, false, false)
		}, "USER_TABLE");
		try{
			catalog.createDatabase(schema);
		} catch (KeyDuplicationException e) {
			logger.error(e.getMessage());
			Assert.fail(e.getMessage());
		}
		
		Schema readSchema = catalog.getSchemaByDatabaseName("USER_TABLE");
		for (int i=0;i<readSchema.getColumns().length;i++){
			logger.debug(readSchema.getColumns()[i]);
		}
	}
	
}
