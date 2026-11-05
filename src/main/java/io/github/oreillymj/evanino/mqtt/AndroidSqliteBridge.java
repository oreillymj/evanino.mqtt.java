/*******************************************************************************
 * Copyright (c) 2026 Michael OReilly
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

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

import java.io.File;

/**
 * {@link SqliteBridge} over Android's {@link SQLiteDatabase}, so {@link MqttDbPersistence} can
 * keep the client's session state in an app's SQLite database:
 * <pre>{@code
 * SqliteBridge db = AndroidSqliteBridge.open(new File(context.getFilesDir(), "mqtt.db"));
 * MqttAsyncClient client = new MqttAsyncClient(serverURI, clientId, new MqttDbPersistence(db));
 * }</pre>
 * The database belongs to the caller, who closes it with {@link #close()} when no client uses it.
 * This class needs Android; it is compiled against the Android API but cannot run on a desktop
 * JVM, so it is tested only through {@link MqttDbPersistence} on {@link JdbcSqliteBridge}.
 */
public final class AndroidSqliteBridge implements SqliteBridge {

	private final SQLiteDatabase db;

	/**
	 * Wraps an open database, for example one from an {@code SQLiteOpenHelper}.
	 *
	 * @param db the database
	 */
	public AndroidSqliteBridge(SQLiteDatabase db) {
		this.db = db;
	}

	/**
	 * Opens, or creates, a database file.
	 *
	 * @param file the database file, typically in {@code context.getFilesDir()}
	 * @return the bridge
	 */
	public static AndroidSqliteBridge open(File file) {
		return new AndroidSqliteBridge(SQLiteDatabase.openOrCreateDatabase(file, null));
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Byte arrays are bound as blobs.
	 */
	@Override
	public void execSQL(String sql, Object[] bindArgs) {
		if (bindArgs == null || bindArgs.length == 0) {
			db.execSQL(sql);   // Android refuses an empty bindArgs array
		} else {
			db.execSQL(sql, bindArgs);
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Android binds query arguments as strings only, so every argument must be a
	 * {@code String}.
	 *
	 * @throws IllegalArgumentException if an argument is not a {@code String}
	 */
	@Override
	public SqliteCursorBridge rawQuery(String sql, Object[] bindArgs) {
		String[] args = null;
		if (bindArgs != null) {
			args = new String[bindArgs.length];
			for (int i = 0; i < bindArgs.length; i++) {
				if (!(bindArgs[i] instanceof String)) {
					throw new IllegalArgumentException("Query argument " + i + " is not a String: " + bindArgs[i]);
				}
				args[i] = (String) bindArgs[i];
			}
		}
		Cursor cursor = db.rawQuery(sql, args);
		return new AndroidCursorBridge(cursor);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void beginTransaction() {
		db.beginTransaction();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setTransactionSuccessful() {
		db.setTransactionSuccessful();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void endTransaction() {
		db.endTransaction();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		db.close();
	}

	/**
	 * {@link SqliteCursorBridge} over an Android {@link Cursor}.
	 */
	private static final class AndroidCursorBridge implements SqliteCursorBridge {
		private final Cursor cursor;

		/**
		 * Wraps a cursor, which this bridge closes.
		 *
		 * @param cursor the cursor
		 */
		AndroidCursorBridge(Cursor cursor) {
			this.cursor = cursor;
		}

		/** {@inheritDoc} */
		@Override
		public boolean moveToNext() {
			return cursor.moveToNext();
		}

		/** {@inheritDoc} */
		@Override
		public String getString(int columnIndex) {
			return cursor.getString(columnIndex);
		}

		/** {@inheritDoc} */
		@Override
		public byte[] getBlob(int columnIndex) {
			return cursor.getBlob(columnIndex);
		}

		/** {@inheritDoc} */
		@Override
		public void close() {
			cursor.close();
		}
	}
}
