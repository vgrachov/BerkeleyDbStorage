package org.brackit.berkeleydb.tpch;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.Schema;
import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.cursor.FullIndexIterator;
import org.brackit.berkeleydb.cursor.ITupleCursor;
import org.brackit.berkeleydb.cursor.RangeIndexSearchCursor;
import org.brackit.berkeleydb.cursor.TupleCursor;
import org.brackit.berkeleydb.cursor.TupleCursor.CursorType;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.AtomicDouble;
import org.brackit.berkeleydb.tuple.Tuple;
import org.junit.AfterClass;
import org.junit.Test;

public class LINEITEM_TABLE_ITERATOR {

	private static final Logger logger = Logger.getLogger(LINEITEM_TABLE_ITERATOR.class);
	private static String tableName = "LINEITEM"; 
	
	@Test
	public void fullIterator(){
		logger.debug("Full scan");
		ITupleCursor cursor = new TupleCursor(tableName,CursorType.FullScan);
		cursor.open();
		Tuple tuple = null;
		while ((tuple = cursor.next())!=null){
			logger.debug(tuple);
		}
		cursor.close();
	}
	
	@Test
	public void fullIndexIterator(){
		logger.debug("Full index scan");
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(tableName);
		ITupleCursor cursor = new FullIndexIterator(schema.getColumns()[6]);
		cursor.open();
		Tuple tuple = null;
		while ((tuple = cursor.next())!=null){
			logger.debug(tuple.getFields()[6]);
		}
		cursor.close();
	}

	@Test
	public void rangeSearch(){
		logger.debug("Range index scan");
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(tableName);
		AtomicDouble leftRange = new AtomicDouble("L_DISCOUNT", 0.02);
		AtomicDouble rightRange = new AtomicDouble("L_DISCOUNT", 0.03);
		ITupleCursor cursor = new RangeIndexSearchCursor(schema.getColumns()[6], leftRange, rightRange);
		cursor.open();
		Tuple tuple = null;
		while ((tuple = cursor.next())!=null){
			logger.debug(tuple.getFields()[6]);
		}
		cursor.close();
	}

	@Test
	public void fullIteratorDateIndex(){
		logger.debug("Full index scan");
		Schema schema = Catalog.getInstance().getSchemaByDatabaseName(tableName);
		ITupleCursor cursor = new FullIndexIterator(schema.getColumns()[10]);
		cursor.open();
		Tuple tuple = null;
		while ((tuple = cursor.next())!=null){
			logger.debug(tuple.getFields()[10]);
		}
		cursor.close();
	}
	
	@AfterClass
	public static void close(){
		BerkeleyDBEnvironment.getInstance().close();
	}
	
}
