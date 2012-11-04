package org.brackit.berkeleydb.tpch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.Catalog;
import org.brackit.berkeleydb.DatabaseAccess;
import org.brackit.berkeleydb.IDatabaseAccess;
import org.brackit.berkeleydb.Schema;
import org.brackit.berkeleydb.impl.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.tuple.Atomic;
import org.brackit.berkeleydb.tuple.AtomicChar;
import org.brackit.berkeleydb.tuple.AtomicDate;
import org.brackit.berkeleydb.tuple.AtomicDouble;
import org.brackit.berkeleydb.tuple.AtomicInteger;
import org.brackit.berkeleydb.tuple.AtomicString;
import org.brackit.berkeleydb.tuple.Column;
import org.brackit.berkeleydb.tuple.ColumnType;
import org.brackit.berkeleydb.tuple.Tuple;
import org.junit.AfterClass;
import org.junit.Test;

public class LINEITEM_TABLE_CREATE {

	private Logger logger = Logger.getLogger(LINEITEM_TABLE_CREATE.class);
	
	@Test
	public void createTable(){
		logger.debug("Create LINEITEM table");
		Column[] columns = new Column[]{
				new Column("LINEITEM", "L_ORDERKEY", ColumnType.Integer, true, true),
				new Column("LINEITEM", "L_PARTKEY", ColumnType.Integer, true, true),
				new Column("LINEITEM", "L_SUPPKEY", ColumnType.Integer, true, true),
				new Column("LINEITEM", "L_LINENUMBER", ColumnType.Integer, false, false),
				new Column("LINEITEM", "L_QUANTITY", ColumnType.Double, false, false),
				new Column("LINEITEM", "L_EXTENDEDPRICE", ColumnType.Double, false, false),
				new Column("LINEITEM", "L_DISCOUNT", ColumnType.Double, false, true),
				new Column("LINEITEM", "L_TAX", ColumnType.Double, false, false),
				new Column("LINEITEM", "L_RETURNFLAG", ColumnType.Char, false, false),
				new Column("LINEITEM", "L_LINESTATUS", ColumnType.Char, false, false),
				new Column("LINEITEM", "L_SHIPDATE", ColumnType.Date, false, true),
				new Column("LINEITEM", "L_COMMITDATE", ColumnType.Date, false, false),
				new Column("LINEITEM", "L_RECEIPTDATE", ColumnType.Date, false, false),
				new Column("LINEITEM", "L_SHIPINSTRUCT", ColumnType.String, false, false),
				new Column("LINEITEM", "L_SHIPMODE", ColumnType.String, false, true),
				new Column("LINEITEM", "L_COMMENT", ColumnType.String, false, false),
		};
		Schema schema = new Schema(columns, "LINEITEM");
		Catalog.getInstance().createDatabase(schema);
		logger.debug("Table create is finish");
	}
	
	@Test
	public void fillTable(){
		IDatabaseAccess databaseAccess = new DatabaseAccess("LINEITEM");
		BufferedReader lineItemInput = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResourceAsStream("tpc-h/100KB_data/lineitem.tbl")));
		//1|155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|
		String line = null;
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			try{
				Date d = dateFormat.parse("1996-03-13");
				System.out.println(d);
			}catch (Exception e) {
				logger.error(e.getMessage());
			}
			while ((line=lineItemInput.readLine())!=null){
				logger.debug(line);
				String[] entries = line.split("\\|");
				Atomic[] fields = new Atomic[16];
				fields[0] = new AtomicInteger("L_ORDERKEY", Integer.valueOf(entries[0]));
				fields[1] = new AtomicInteger("L_PARTKEY", Integer.valueOf(entries[1]));
				fields[2] = new AtomicInteger("L_SUPPKEY", Integer.valueOf(entries[2]));
				fields[3] = new AtomicInteger("L_LINENUMBER", Integer.valueOf(entries[3]));
				fields[4] = new AtomicDouble("L_QUANTITY", Double.valueOf(entries[4]));
				fields[5] = new AtomicDouble("L_EXTENDEDPRICE", Double.valueOf(entries[5]));
				fields[6] = new AtomicDouble("L_DISCOUNT", Double.valueOf(entries[6]));
				fields[7] = new AtomicDouble("L_TAX", Double.valueOf(entries[7]));
				fields[8] = new AtomicChar("L_RETURNFLAG", entries[8].charAt(0));
				fields[9] = new AtomicChar("L_LINESTATUS", entries[9].charAt(0));
				try{
					fields[10] = new AtomicDate("L_SHIPDATE", dateFormat.parse(entries[10]));
					fields[11] = new AtomicDate("L_COMMITDATE", dateFormat.parse(entries[11]));
					fields[12] = new AtomicDate("L_RECEIPTDATE", dateFormat.parse(entries[12]));
				}catch (Exception e) {
					logger.error(e.getMessage());
				}
				fields[13] = new AtomicString("L_SHIPINSTRUCT", entries[13]);
				fields[14] = new AtomicString("L_SHIPMODE", entries[14]);
				fields[15] = new AtomicString("L_COMMENT", entries[15]);
				Tuple tuple = new Tuple(fields);
				databaseAccess.insert(tuple);
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	@AfterClass
	public static void close(){
		BerkeleyDBEnvironment.getInstance().close();
	}
	
}
/*CREATE TEXT TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
        L_PARTKEY     INTEGER NOT NULL,
        L_SUPPKEY     INTEGER NOT NULL,
        L_LINENUMBER  INTEGER NOT NULL,
        L_QUANTITY    DECIMAL(15,2) NOT NULL,
        L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
        L_DISCOUNT    DECIMAL(15,2) NOT NULL,
        L_TAX         DECIMAL(15,2) NOT NULL,
        L_RETURNFLAG  CHAR(1) NOT NULL,
        L_LINESTATUS  CHAR(1) NOT NULL,
        L_SHIPDATE    DATE NOT NULL,
        L_COMMITDATE  DATE NOT NULL,
        L_RECEIPTDATE DATE NOT NULL,
        L_SHIPINSTRUCT CHAR(25) NOT NULL,
        L_SHIPMODE     CHAR(10) NOT NULL,
        L_COMMENT      VARCHAR(44) NOT NULL);
*/