package org.brackit.berkeleydb.catalog;

import org.brackit.berkeleydb.Schema;
import org.brackit.berkeleydb.exception.KeyDuplicationException;

public interface ICatalog {

	/**
	 * Create Primary database with all indexes, represented in secondary databases
	 * @param schema - database schema
	 */
	public abstract void createDatabase(Schema schema)
			throws KeyDuplicationException;

	public abstract Schema getSchemaByDatabaseName(String databaseName);

	/**
	 * Delete database with all indexes
	 * @param databaseName
	 */
	public abstract boolean deleteDatabase(String databaseName);

}