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

import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@link MqttDbPersistence} on a SQLite database through {@link JdbcSqliteBridge}. On Android the
 * same class runs on {@link AndroidSqliteBridge}, which needs a device to test. Not part of
 * Paho's suite; needs no broker.
 */
public class MqttDbPersistenceTest {

	/**
	 * A directory for the database file.
	 */
	@Rule
	public final TemporaryFolder folder = new TemporaryFolder();

	private File dbFile;
	private JdbcSqliteBridge db;

	/**
	 * Opens a new database.
	 *
	 * @throws Exception if it cannot be created
	 */
	@Before
	public void setUp() throws Exception {
		dbFile = new File(folder.getRoot(), "mqtt.db");
		db = new JdbcSqliteBridge(dbFile.getPath());
	}

	/**
	 * Closes the database.
	 *
	 * @throws Exception if it cannot be closed
	 */
	@After
	public void tearDown() throws Exception {
		db.close();
	}

	/**
	 * Entries can be stored, read back (also when their bytes start at an offset), replaced,
	 * listed and removed.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testPutGetReplaceRemove() throws Exception {
		MqttDbPersistence p = new MqttDbPersistence(db);
		p.open("client", "tcp://broker:1883");
		p.put("s-1", new MqttPersistable(new byte[]{1, 2}, new byte[]{3}));
		p.put("s-2", new Sliced(new byte[]{9, 9, 4, 5, 9}, 2, 2, new byte[]{9, 6, 7}, 1, 2));

		assertEntry(p.get("s-1"), new byte[]{1, 2}, new byte[]{3});
		assertEntry(p.get("s-2"), new byte[]{4, 5}, new byte[]{6, 7});
		Assert.assertEquals(sorted("s-1", "s-2"), keys(p));

		p.put("s-1", new MqttPersistable(new byte[]{8}, new byte[0]));
		assertEntry(p.get("s-1"), new byte[]{8}, new byte[0]);

		p.remove("s-1");
		Assert.assertEquals(sorted("s-2"), keys(p));
	}

	/**
	 * Clients sharing one database see only their own entries, and {@code clear()} removes only
	 * the caller's.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testEntriesAreScopedByClientAndBroker() throws Exception {
		MqttDbPersistence a = new MqttDbPersistence(db);
		MqttDbPersistence b = new MqttDbPersistence(db);
		a.open("client-a", "tcp://broker:1883");
		b.open("client-b", "tcp://broker:1883");
		a.put("s-1", new MqttPersistable(new byte[]{1}, new byte[]{1}));
		b.put("s-1", new MqttPersistable(new byte[]{2}, new byte[]{2}));

		assertEntry(a.get("s-1"), new byte[]{1}, new byte[]{1});
		assertEntry(b.get("s-1"), new byte[]{2}, new byte[]{2});
		a.clear();
		Assert.assertTrue(keys(a).isEmpty());
		Assert.assertEquals(sorted("s-1"), keys(b));
	}

	/**
	 * {@code close()} keeps the entries and leaves the database open, so the same instance can
	 * be opened again, and a new connection to the same file (as after a restart) sees them.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testCloseKeepsEntriesAndDatabase() throws Exception {
		MqttDbPersistence p = new MqttDbPersistence(db);
		p.open("client", "tcp://broker:1883");
		p.put("r-5", new MqttPersistable(new byte[]{5}, new byte[]{5}));
		p.close();

		p.open("client", "tcp://broker:1883");
		Assert.assertEquals(sorted("r-5"), keys(p));

		JdbcSqliteBridge restarted = new JdbcSqliteBridge(dbFile.getPath());
		try {
			MqttDbPersistence q = new MqttDbPersistence(restarted);
			q.open("client", "tcp://broker:1883");
			assertEntry(q.get("r-5"), new byte[]{5}, new byte[]{5});
		} finally {
			restarted.close();
		}
	}

	/**
	 * Reading a key that is not stored fails.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(expected = MqttPersistenceException.class)
	public void testMissingKeyFails() throws Exception {
		MqttDbPersistence p = new MqttDbPersistence(db);
		p.open("client", "tcp://broker:1883");
		p.get("s-404");
	}

	/**
	 * As on Android, a transaction is committed only if marked successful, and rolled back
	 * otherwise.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testJdbcTransactionsCommitOnlyWhenSuccessful() throws Exception {
		db.execSQL("CREATE TABLE t (v INTEGER)", null);
		db.beginTransaction();
		db.execSQL("INSERT INTO t VALUES (?)", new Object[]{1});
		db.endTransaction();   // not marked successful
		Assert.assertEquals(0, count());

		db.beginTransaction();
		db.execSQL("INSERT INTO t VALUES (?)", new Object[]{2});
		db.setTransactionSuccessful();
		db.endTransaction();
		Assert.assertEquals(1, count());
	}

	/**
	 * Counts the rows of the transaction test's table.
	 *
	 * @return the number of rows
	 * @throws Exception if the query fails
	 */
	private int count() throws Exception {
		try (SqliteCursorBridge c = db.rawQuery("SELECT COUNT(*) FROM t", null)) {
			Assert.assertTrue(c.moveToNext());
			return Integer.parseInt(c.getString(0));
		}
	}

	/**
	 * Checks an entry's header and payload.
	 *
	 * @param p       the entry
	 * @param header  the expected header
	 * @param payload the expected payload
	 * @throws MqttPersistenceException never, for these entries
	 */
	private static void assertEntry(IMqttPersistable p, byte[] header, byte[] payload) throws MqttPersistenceException {
		byte[] h = new byte[p.getHeaderLength()];
		System.arraycopy(p.getHeaderBytes(), p.getHeaderOffset(), h, 0, h.length);
		byte[] b = new byte[p.getPayloadLength()];
		if (b.length > 0) {
			System.arraycopy(p.getPayloadBytes(), p.getPayloadOffset(), b, 0, b.length);
		}
		Assert.assertArrayEquals(header, h);
		Assert.assertArrayEquals(payload, b);
	}

	/**
	 * Lists a persistence's keys, sorted.
	 *
	 * @param p the persistence
	 * @return the keys
	 * @throws MqttPersistenceException if they cannot be read
	 */
	private static List<String> keys(MqttDbPersistence p) throws MqttPersistenceException {
		List<String> keys = Collections.list(p.keys());
		Collections.sort(keys);
		return keys;
	}

	/**
	 * Returns the arguments as a sorted list.
	 *
	 * @param keys the keys
	 * @return the list
	 */
	private static List<String> sorted(String... keys) {
		List<String> list = new ArrayList<>(java.util.Arrays.asList(keys));
		Collections.sort(list);
		return list;
	}

	/**
	 * An entry whose bytes are slices of larger arrays.
	 */
	private static final class Sliced implements IMqttPersistable {
		private final byte[] header;
		private final int headerOffset;
		private final int headerLength;
		private final byte[] payload;
		private final int payloadOffset;
		private final int payloadLength;

		Sliced(byte[] header, int headerOffset, int headerLength, byte[] payload, int payloadOffset, int payloadLength) {
			this.header = header;
			this.headerOffset = headerOffset;
			this.headerLength = headerLength;
			this.payload = payload;
			this.payloadOffset = payloadOffset;
			this.payloadLength = payloadLength;
		}

		@Override
		public byte[] getHeaderBytes() {
			return header;
		}

		@Override
		public int getHeaderOffset() {
			return headerOffset;
		}

		@Override
		public int getHeaderLength() {
			return headerLength;
		}

		@Override
		public byte[] getPayloadBytes() {
			return payload;
		}

		@Override
		public int getPayloadOffset() {
			return payloadOffset;
		}

		@Override
		public int getPayloadLength() {
			return payloadLength;
		}
	}
}
