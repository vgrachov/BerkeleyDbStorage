package org.brackit.berkeleydb.tpch;

import org.brackit.relational.api.transaction.ITransaction;
import org.brackit.relational.api.transaction.ITransactionManager;
import org.brackit.relational.api.transaction.IsolationLevel;
import org.brackit.relational.api.transaction.TransactionException;
import org.brackit.relational.api.transaction.impl.TransactionManager;
import org.junit.Assert;

public class BasicTCPHTest {

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
	
}
