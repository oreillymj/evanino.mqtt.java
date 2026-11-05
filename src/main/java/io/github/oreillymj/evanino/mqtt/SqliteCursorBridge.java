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
 * Minimal, platform-neutral view of a SQLite query cursor, so the persistence layer can
 * run on both Android and JDBC-backed SQLite.
 */
public interface SqliteCursorBridge extends AutoCloseable {
	/**
	 * Advances the cursor to the next row.
	 *
	 * @return {@code true} if the cursor now points at a row, {@code false} if there are no more rows
	 * @throws Exception if the underlying cursor fails
	 */
	boolean moveToNext() throws Exception;

	/**
	 * Reads a column of the current row as a string.
	 *
	 * @param columnIndex zero-based column index
	 * @return the column value, or {@code null} if it is SQL {@code NULL}
	 * @throws Exception if the underlying cursor fails
	 */
	String getString(int columnIndex) throws Exception;

	/**
	 * Reads a column of the current row as a byte array.
	 *
	 * @param columnIndex zero-based column index
	 * @return the column value, or {@code null} if it is SQL {@code NULL}
	 * @throws Exception if the underlying cursor fails
	 */
	byte[] getBlob(int columnIndex) throws Exception;

	/**
	 * Releases the cursor. Unlike {@link AutoCloseable#close()}, this does not throw.
	 */
	@Override
	void close();
}