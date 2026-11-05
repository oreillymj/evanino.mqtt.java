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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * {@link MqttClientPersistence} backed by a SQLite table. Entries are scoped by client ID and
 * server URI, so several clients can share one database. All methods are synchronized.
 */
public class MqttDbPersistence implements MqttClientPersistence {

	private final SqliteBridge db;
	private volatile String clientId;
	private volatile String serverURI;
	private volatile boolean opened = false;


	private static final String SQL_CREATE_TABLE =
			"CREATE TABLE IF NOT EXISTS mqtt_persistence (" +
					" client_id TEXT NOT NULL, " +
					" server_uri TEXT NOT NULL, " +
					" pkey TEXT NOT NULL, " +
					" header_bytes BLOB, " +
					" payload_bytes BLOB, " +
					" PRIMARY KEY (client_id, server_uri, pkey))";

	private static final String SQL_UPSERT =
			"INSERT OR REPLACE INTO mqtt_persistence (client_id, server_uri, pkey, header_bytes, payload_bytes) VALUES (?, ?, ?, ?, ?)";
	private static final String SQL_SELECT =
			"SELECT header_bytes, payload_bytes FROM mqtt_persistence WHERE client_id = ? AND server_uri = ? AND pkey = ?";
	private static final String SQL_DELETE_ONE =
			"DELETE FROM mqtt_persistence WHERE client_id = ? AND server_uri = ? AND pkey = ?";
	private static final String SQL_SELECT_KEYS =
			"SELECT pkey FROM mqtt_persistence WHERE client_id = ? AND server_uri = ?";
	private static final String SQL_DELETE_ALL_FOR_SESSION =
			"DELETE FROM mqtt_persistence WHERE client_id = ? AND server_uri = ?";

	/**
	 * Creates a persistence store over the given database.
	 *
	 * @param db the database to store entries in
	 */
	public MqttDbPersistence(SqliteBridge db) {
		this.db = db;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Creates the {@code mqtt_persistence} table if it does not exist, and scopes later
	 * calls to the given client and server.
	 */
	@Override
	public synchronized void open(String clientId, String serverURI) throws MqttPersistenceException {
		try {
			this.clientId = clientId;
			this.serverURI = serverURI;
			db.execSQL(
					SQL_CREATE_TABLE,
					null);
			opened = true;
		} catch (Exception ex) {
			throw new MqttPersistenceException(ex);
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * The stored entries are kept for the next client, and the database stays open: it belongs
	 * to the caller, who closes the {@link SqliteBridge} when no client uses it.
	 */
	@Override
	public synchronized void close() {
		opened = false;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * The header and payload are copied from their arrays using the persistable's offsets
	 * and lengths.
	 *
	 * @throws MqttPersistenceException if the store is not open or the write fails
	 */
	@Override
	public synchronized void put(String key, IMqttPersistable persistable) throws MqttPersistenceException {
		requireOpen();
		try {
			byte[] header = sliceOrEmpty(persistable.getHeaderBytes(), persistable.getHeaderOffset(), persistable.getHeaderLength());
			byte[] payload = sliceOrEmpty(persistable.getPayloadBytes(), persistable.getPayloadOffset(), persistable.getPayloadLength());
			// INSERT OR REPLACE — a retransmission re-persisting the same key
			// (same client/server/pkey) should overwrite, not error or duplicate
			db.execSQL(
					SQL_UPSERT,
					new Object[]{clientId, serverURI, key, header, payload});
		} catch (Exception ex) {
			throw new MqttPersistenceException(ex);
		}
	}

	/**
	 * {@inheritDoc}
	 *
	 * @throws MqttPersistenceException if the store is not open, there is no entry for the
	 *                                  key, or the read fails
	 */
	@Override
	public synchronized IMqttPersistable get(String key) throws MqttPersistenceException {
		requireOpen();
		try (SqliteCursorBridge cursor = db.rawQuery(
				SQL_SELECT,
				new Object[]{clientId, serverURI, key})) {
			if (!cursor.moveToNext()) {
				throw new MqttPersistenceException("No persisted record for key: " + key);
			}
			byte[] header = cursor.getBlob(0);
			byte[] payload = cursor.getBlob(1);
			return new MqttPersistable(header, payload);
		} catch (MqttPersistenceException mpe) {
			throw mpe;
		} catch (Exception ex) {
			throw new MqttPersistenceException(ex);
		}
	}

	/**
	 * {@inheritDoc}
	 *
	 * @throws MqttPersistenceException if the store is not open or the delete fails
	 */
	@Override
	public synchronized void remove(String key) throws MqttPersistenceException {
		requireOpen();
		try {
			db.execSQL(
					SQL_DELETE_ONE,
					new Object[]{clientId, serverURI, key});
		} catch (Exception ex) {
			throw new MqttPersistenceException(ex);
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Only keys belonging to the client and server given to {@link #open(String, String)}
	 * are returned.
	 *
	 * @throws MqttPersistenceException if the store is not open or the read fails
	 */
	@Override
	public synchronized Enumeration<String> keys() throws MqttPersistenceException {
		requireOpen();
		List<String> result = new ArrayList<>();
		try (SqliteCursorBridge cursor = db.rawQuery(
				SQL_SELECT_KEYS,
				new Object[]{clientId, serverURI})) {
			while (cursor.moveToNext()) {
				result.add(cursor.getString(0));
			}
		} catch (Exception ex) {
			throw new MqttPersistenceException(ex);
		}
		return Collections.enumeration(result);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Only entries belonging to the client and server given to {@link #open(String, String)}
	 * are removed.
	 *
	 * @throws MqttPersistenceException if the store is not open or the delete fails
	 */
	@Override
	public synchronized void clear() throws MqttPersistenceException {
		requireOpen();
		try {
			// this is the clean-session / sessionPresent==false purge hook —
			// callers invoke this BEFORE connect() proceeds when a fresh session
			// is required, or after a CONNACK reports sessionPresent==false
			// despite requesting cleanSession==false
			db.execSQL(
					SQL_DELETE_ALL_FOR_SESSION,
					new Object[]{clientId, serverURI});
		} catch (Exception ex) {
			throw new MqttPersistenceException(ex);
		}
	}

	/**
	 * Checks that {@link #open(String, String)} has been called.
	 *
	 * @throws MqttPersistenceException if the store is not open
	 */
	private void requireOpen() throws MqttPersistenceException {
		if (!opened) {
			throw new MqttPersistenceException("Persistence store is not open");
		}
	}

	/**
	 * Copies a range of an array.
	 *
	 * @param src    the source array
	 * @param offset the start of the range
	 * @param length the number of bytes to copy
	 * @return a new array holding the range, or an empty array if {@code src} is {@code null}
	 * or {@code length} is not positive
	 */
	private static byte[] sliceOrEmpty(byte[] src, int offset, int length) {
		if (src == null || length <= 0) return new byte[0];
		byte[] out = new byte[length];
		System.arraycopy(src, offset, out, 0, length);
		return out;
	}
}