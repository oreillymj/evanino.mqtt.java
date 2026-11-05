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

import java.sql.*;

/**
 * {@link SqliteBridge} backed by a JDBC SQLite driver, for use on desktop and server JVMs.
 * A SQLite JDBC driver must be on the classpath.
 */
public class JdbcSqliteBridge implements SqliteBridge {

	private final Connection connection;

	/** Whether {@link #setTransactionSuccessful()} was called in the current transaction. */
	private boolean transactionSuccessful;

	/**
	 * Opens, or creates, a SQLite database file.
	 *
	 * @param dbFilePath the path of the database file
	 * @throws SQLException if the database cannot be opened
	 */
	public JdbcSqliteBridge(String dbFilePath) throws SQLException {
		this.connection = DriverManager.getConnection("jdbc:sqlite:" + dbFilePath);
	}

	/** {@inheritDoc} */
	@Override
	public void execSQL(String sql, Object[] bindArgs) throws SQLException {
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			bind(stmt, bindArgs);
			stmt.execute();
		}
	}

	/** {@inheritDoc} */
	@Override
	public SqliteCursorBridge rawQuery(String sql, Object[] bindArgs) throws SQLException {
		PreparedStatement stmt = connection.prepareStatement(sql);
		bind(stmt, bindArgs);
		ResultSet rs = stmt.executeQuery();
		return new JdbcCursorBridge(stmt, rs);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Implemented by turning off auto-commit.
	 */
	@Override
	public void beginTransaction() throws SQLException {
		connection.setAutoCommit(false);
		transactionSuccessful = false;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * As on Android, the changes are committed by {@link #endTransaction()}.
	 */
	@Override
	public void setTransactionSuccessful() {
		transactionSuccessful = true;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * As on Android, commits the transaction if {@link #setTransactionSuccessful()} was
	 * called and rolls it back otherwise, then turns auto-commit back on.
	 */
	@Override
	public void endTransaction() throws SQLException {
		try {
			if (transactionSuccessful) {
				connection.commit();
			} else {
				connection.rollback();
			}
		} finally {
			transactionSuccessful = false;
			connection.setAutoCommit(true);
		}
	}

	/** {@inheritDoc} */
	@Override
	public void close() throws SQLException {
		connection.close();
	}

	/**
	 * Binds arguments to a statement's placeholders, in order.
	 *
	 * @param stmt the statement
	 * @param args the values to bind; {@code null} binds nothing
	 * @throws SQLException if a value cannot be bound
	 */
	private void bind(PreparedStatement stmt, Object[] args) throws SQLException {
		if (args == null) return;
		for (int i = 0; i < args.length; i++) {
			stmt.setObject(i + 1, args[i]);
		}
	}

	/**
	 * {@link SqliteCursorBridge} over a JDBC result set. Column indexes are zero-based, as on
	 * Android, and translated to JDBC's one-based indexes.
	 */
	private static class JdbcCursorBridge implements SqliteCursorBridge {
		private final PreparedStatement stmt;
		private final ResultSet rs;

		/**
		 * Creates a cursor that owns, and closes, the given statement and result set.
		 *
		 * @param stmt the statement that produced the result set
		 * @param rs   the result set to iterate
		 */
		JdbcCursorBridge(PreparedStatement stmt, ResultSet rs) {
			this.stmt = stmt;
			this.rs = rs;
		}

		/** {@inheritDoc} */
		@Override
		public boolean moveToNext() throws SQLException {
			return rs.next();
		}

		/** {@inheritDoc} */
		@Override
		public String getString(int col) throws SQLException {
			return rs.getString(col + 1);
		}

		/** {@inheritDoc} */
		@Override
		public byte[] getBlob(int col) throws SQLException {
			return rs.getBytes(col + 1);
		}

		/**
		 * Closes the result set and statement, ignoring any errors.
		 */
		@Override
		public void close() {
			try {
				rs.close();
			} catch (SQLException ignored) {
			}
			try {
				stmt.close();
			} catch (SQLException ignored) {
			}
		}
	}
}