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
