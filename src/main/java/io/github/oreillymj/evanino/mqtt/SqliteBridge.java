/*******************************************************************************
 * Copyright (c) 2017, 2026 Michael OReilly
 *
 * This program and the accompanying materials are made available under
 * the terms of the Apache License, Version 2.0, which accompanies this
 * distribution (see LICENSE).
 *
 * The Apache License is available at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Contributors:
 *    Michael OReilly - initial API and implementation
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

/**
 * Minimal, platform-neutral view of a SQLite database, so the persistence layer can run on
 * both Android and JDBC-backed SQLite.
 */
public interface SqliteBridge {
	/**
	 * Executes a statement that returns no rows.
	 *
	 * @param sql      the SQL statement, using {@code ?} placeholders
	 * @param bindArgs values bound to the placeholders, in order
	 * @throws Exception if the statement fails
	 */
	void execSQL(String sql, Object[] bindArgs) throws Exception;

	/**
	 * Runs a query and returns a cursor over the result rows.
	 *
	 * @param sql      the SQL query, using {@code ?} placeholders
	 * @param bindArgs values bound to the placeholders, in order
	 * @return a cursor over the results, which the caller must close
	 * @throws Exception if the query fails
	 */
	SqliteCursorBridge rawQuery(String sql, Object[] bindArgs) throws Exception;

	/**
	 * Starts a transaction.
	 *
	 * @throws Exception if the transaction cannot be started
	 */
	void beginTransaction() throws Exception;

	/**
	 * Marks the current transaction as successful, so {@code endTransaction()} commits it.
	 *
	 * @throws Exception if the transaction state cannot be updated
	 */
	void setTransactionSuccessful() throws Exception;

	/**
	 * Ends the current transaction, committing it if it was marked successful and rolling it
	 * back otherwise.
	 *
	 * @throws Exception if the transaction cannot be ended
	 */
	void endTransaction() throws Exception;

	/**
	 * Closes the database.
	 *
	 * @throws Exception if the database cannot be closed
	 */
	void close() throws Exception;
}
