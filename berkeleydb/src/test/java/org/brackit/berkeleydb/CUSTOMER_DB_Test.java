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

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.Atomic;
import org.brackit.berkeleydb.tuple.AtomicDouble;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;
import org.brackit.berkeleydb.tuple.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseEntry;

public class CUSTOMER_DB_Test {

	private Logger logger = Logger.getLogger(CUSTOMER_DB_Test.class);
	private static DatabaseAccess customerDatabase;
	
	@BeforeClass
	public static void init(){
		customerDatabase = new DatabaseAccess("CUSTOMER");
	}
	
	@Test
	public void insert() {
		BufferedReader customerInput = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResourceAsStream("tpc-h/100KB_data/customer.tbl")));
		String line = null;
		//1|Customer#000000001|IVhzIApeRb ot,c,E|15|25-989-741-2988|711.56|BUILDING|to the even, regular platelets. regular, ironic epitaphs nag e|
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName("CUSTOMER");
		Column[] columns = schema.getColumns();
		DatabaseAccess databaseAccess = new DatabaseAccess("CUSTOMER");
		try {
			int index=0;
			while ((line = customerInput.readLine())!=null){
				Atomic[] fields = new Atomic[columns.length];
				logger.debug(line);
				String[] entries = line.split("\\|");
				for (int i=0;i<columns.length;i++){
					if (columns[i].getType()==ColumnType.String)
						fields[i] = new AtomicString(columns[i].getColumnName(), entries[i]);
					else
					if (columns[i].getType()==ColumnType.Integer)
						fields[i] = new AtomicInteger(columns[i].getColumnName(), Integer.valueOf(entries[i]));
					else
					if (columns[i].getType()==ColumnType.Double)
						fields[i] = new AtomicDouble(columns[i].getColumnName(), Double.valueOf(entries[i]));
				}
				DatabaseEntry key = new DatabaseEntry();
				databaseAccess.insert(new Tuple(fields));
				index++;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	@AfterClass
	public static void clean(){
		BerkeleyDBEnvironment.getInstance().close();
	}

}
/*CREATE TEXT TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
        C_NAME        VARCHAR(25) NOT NULL,
        C_ADDRESS     VARCHAR(40) NOT NULL,
        C_NATIONKEY   INTEGER NOT NULL,
        C_PHONE       CHAR(15) NOT NULL,
        C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
        C_MKTSEGMENT  CHAR(10) NOT NULL,
        C_COMMENT     VARCHAR(117) NOT NULL);
*/
