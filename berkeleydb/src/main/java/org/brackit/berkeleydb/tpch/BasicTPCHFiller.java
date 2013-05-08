package org.brackit.berkeleydb.tpch;

import org.brackit.berkeleydb.catalog.Catalog;
import org.brackit.berkeleydb.environment.BerkeleyDBEnvironment;
import org.brackit.berkeleydb.environment.IBerkeleyDBEnvironment;
import org.brackit.relational.api.ICatalog;
import org.brackit.relational.api.transaction.ITransaction;
import org.brackit.relational.api.transaction.ITransactionManager;
import org.brackit.relational.api.transaction.IsolationLevel;
import org.brackit.relational.api.transaction.TransactionException;
import org.brackit.relational.api.transaction.impl.TransactionManager;

public abstract class BasicTPCHFiller {

	protected final IBerkeleyDBEnvironment berkeleyDBEnvironment;
	protected final ICatalog catalog;

	public BasicTPCHFiller() {
		this.berkeleyDBEnvironment = BerkeleyDBEnvironment.getInstance();
		this.catalog = Catalog.getInstance();
	}
	
	public ITransaction beginTransaction() throws TransactionException{
		ITransactionManager transactionManager = TransactionManager.getInstance();
		ITransaction transaction = transactionManager.begin(IsolationLevel.ReadUnComitted);
		return transaction;
	}

	public void commit(ITransaction transaction) throws TransactionException{
		try {
			if (transaction!=null)
				transaction.commit();
		} catch (TransactionException e) {
			if (transaction != null)
				transaction.abort();
			throw e;
		}
	}
	
	public void close(){
		BerkeleyDBEnvironment.getInstance().close();
	}

	public abstract void createTable();
	
	public abstract void fillTable() throws TransactionException;
	
	public abstract String getTableName();
}
