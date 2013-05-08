package org.brackit.berkeleydb.tpch;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.environment.IBerkeleyDBEnvironment;
import org.brackit.relational.api.transaction.TransactionException;

public class DatabaseFillerRunner {
	
	private static final Logger logger = Logger.getLogger(DatabaseFillerRunner.class);
	
	public static void main(String[] args) throws TransactionException{
		//TODO: move to Guava ImmutableList
		List<BasicTPCHFiller> fillers = new ArrayList<BasicTPCHFiller>();
		fillers.add(new Customer_Table_Create());
		fillers.add(new Lineitem_Table_Create());
		fillers.add(new Supplier_Create_Table());
		fillers.add(new Nation_Table_Create());
		fillers.add(new Orders_Table_Create());
		fillers.add(new Part_Table_Create());
		fillers.add(new Partsupp_Table_Create());
		fillers.add(new Region_Table_Create());
		fillers.add(new Supplier_Create_Table());
		
		IBerkeleyDBEnvironment berkeleyDBEnvironment = BerkeleyDBEnvironment.getInstance();
		for (int i=0; i<fillers.size(); i++) {
			BasicTPCHFiller filler = fillers.get(i);
			logger.info("Create table "+filler.getTableName());
			filler.createTable();
			filler.fillTable();
		}
		berkeleyDBEnvironment.close();
	}
}
