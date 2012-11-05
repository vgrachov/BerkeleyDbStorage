package org.brackit.berkeleydb.tpch;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.Schema;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.catalog.ICatalog;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.exception.KeyDuplicationException;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;
import org.junit.BeforeClass;
import org.junit.Test;

public class CUSTOMER_TABLE_CATALOG {
	
	private static final Logger logger = Logger.getLogger(CUSTOMER_TABLE_CATALOG.class);
	
	private static ICatalog catalog;
	
	@BeforeClass
	public static void init(){
		catalog = Catalog.getInstance();
	}
	
	@Test
	public void deleteCreateDeleteCreateTest(){
		boolean delete = deleteTable();
		createTable();
		delete = deleteTable();
		Assert.assertEquals(delete, true);
		createTable();
	}

	public void createTable(){
		catalog = Catalog.getInstance();
		Schema schema = new Schema(new Column[]{
				new Column("CUSTOMER","C_CUSTKEY", ColumnType.Integer,true,true),
				new Column("CUSTOMER","C_NAME", ColumnType.String,false,true),
				new Column("CUSTOMER","C_ADDRESS", ColumnType.String,false,false),
				new Column("CUSTOMER","C_NATIONKEY", ColumnType.Integer,false,true),
				new Column("CUSTOMER","C_PHONE", ColumnType.String,false,false),
				new Column("CUSTOMER","C_ACCTBAL", ColumnType.Double,false,false),
				new Column("CUSTOMER","C_MKTSEGMENT", ColumnType.String,false,false),
				new Column("CUSTOMER","C_COMMENT", ColumnType.String,false,false)
		}, "CUSTOMER");
		try{
			catalog.createDatabase(schema);
		} catch (KeyDuplicationException e) {
			logger.error(e.getMessage());
			Assert.fail(e.getMessage());
		}
		Schema schema2 = catalog.getSchemaByDatabaseName("CUSTOMER");
		for (int i=0;i<schema2.getColumns().length;i++){
			logger.debug("Print column "+schema2.getColumns()[i]);
		}
		Assert.assertEquals(schema2.getDatabaseName(), "CUSTOMER");
		Assert.assertEquals(schema2.getColumns().length, 8);
		Assert.assertEquals(schema2.getColumns()[0].getColumnName(), "C_CUSTKEY");
		Assert.assertEquals(schema2.getColumns()[1].isDirectIndexExist(), true);
		Assert.assertEquals(schema2.getColumns()[7].getColumnName(), "C_COMMENT");
	}
	
	
	public boolean deleteTable(){
		boolean delete = catalog.deleteDatabase("CUSTOMER");
		return delete;
	}
	
	public static void close(){
		BerkeleyDBEnvironment.getInstance().close();
	}
	
}
