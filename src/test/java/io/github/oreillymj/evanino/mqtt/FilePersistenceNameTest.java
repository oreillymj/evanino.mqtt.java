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

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.Collections;

/**
 * The directory {@link MqttDefaultFilePersistence} keeps a client's state in. It is named after
 * the client ID and server URI; a long client ID used to give a name longer than file systems
 * allow, so the persistence could not be opened (Paho issue 1061). Long names are now
 * shortened with a hash; short ones are unchanged. Not part of Paho's suite; the client test
 * uses {@link MosquittoBroker}.
 */
public class FilePersistenceNameTest {

	/**
	 * The broker.
	 */
	@ClassRule
	public static final MosquittoBroker BROKER = new MosquittoBroker();

	/**
	 * For the stores.
	 */
	@Rule
	public final TemporaryFolder folder = new TemporaryFolder();

	/**
	 * Short names keep the form they always had, so existing stores are found.
	 */
	@Test
	public void testShortNamesUnchanged() {
		Assert.assertEquals("client_1-tcp___localhost_1883",
				MqttDefaultFilePersistence.directoryName("client/1", "tcp://localhost:1883"));
	}

	/**
	 * Long names are at most 64 + 1 + 64 characters, and two client IDs that differ only after
	 * the first 64 characters get different directories.
	 */
	@Test
	public void testLongNamesShortenedAndDistinct() {
		String a = repeat('c', 1000) + "a";
		String b = repeat('c', 1000) + "b";
		String na = MqttDefaultFilePersistence.directoryName(a, "tcp://localhost:1883");
		String nb = MqttDefaultFilePersistence.directoryName(b, "tcp://localhost:1883");
		Assert.assertEquals(129, na.length());
		Assert.assertNotEquals(na, nb);
	}

	/**
	 * A 1000-character client ID can be stored and read back, and a client using it connects
	 * and publishes with a persistent session.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testLongClientId() throws Exception {
		String id = repeat('c', 1000);
		MqttDefaultFilePersistence p = new MqttDefaultFilePersistence(folder.newFolder("long").getAbsolutePath());
		p.open(id, "tcp://a-rather-long-broker-host-name.example.com:1883");
		p.put("s-1", new MqttPersistable(new byte[]{1, 2}, new byte[]{3}));
		Assert.assertEquals(Collections.singletonList("s-1"), Collections.list(p.keys()));
		Assert.assertEquals(2, p.get("s-1").getHeaderLength());
		p.close();
		p.open(id, "tcp://a-rather-long-broker-host-name.example.com:1883");
		Assert.assertEquals(Collections.singletonList("s-1"), Collections.list(p.keys()));
		p.close();

		MqttDefaultFilePersistence store = new MqttDefaultFilePersistence(folder.newFolder("client").getAbsolutePath());
		MqttAsyncClient client = new MqttAsyncClient(BROKER.getServerURI().toString(), id, store);
		try {
			MqttConnectOptions o = TestClients.options(BROKER.getServerURI(), id);
			o.setCleanSession(false);
			client.connect(o).waitForCompletion(10_000);
			client.publish("FilePersistenceName/t", new byte[]{1}, 1, false).waitForCompletion(10_000);
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	private static String repeat(char c, int n) {
		char[] chars = new char[n];
		Arrays.fill(chars, c);
		return new String(chars);
	}
}
