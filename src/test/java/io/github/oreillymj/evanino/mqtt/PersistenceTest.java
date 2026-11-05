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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Session state survives the client: with an {@link MqttClientPersistence}, QoS 1 and 2
 * publishes awaiting acknowledgement and incoming QoS 2 messages awaiting PUBREL are stored, and
 * a client created later with the same ID and store (as after a restart) sends them again and
 * completes them. Before, the client ignored persistence and lost this state with the process.
 * A restart is simulated by closing the client and creating another with the same store. The
 * first client connects through a relay that holds the broker's replies back for
 * {@value #REPLY_DELAY_MILLIS} ms, so its messages are still in flight when it closes. Not part
 * of Paho's suite. Requires a broker; see {@link MosquittoBroker}.
 */
public class PersistenceTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID and topic. */
	@Rule
	public final TestName name = new TestName();

	/** A directory for the file persistence. */
	@Rule
	public final TemporaryFolder folder = new TemporaryFolder();

	/** How long the relay holds back each reply from the broker. */
	private static final long REPLY_DELAY_MILLIS = 1500;

	private static DelayingRelay relay;
	private static URI relayURI;

	/**
	 * Starts the relay in front of the broker.
	 *
	 * @throws Exception if the relay cannot be started
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		URI serverURI = broker.getServerURI();
		relay = new DelayingRelay(serverURI.getHost(), serverURI.getPort(), REPLY_DELAY_MILLIS);
		relayURI = new URI("tcp://127.0.0.1:" + relay.getPort());
	}

	/**
	 * Stops the relay.
	 */
	@AfterClass
	public static void tearDownAfterClass() {
		if (relay != null) {
			relay.close();
		}
	}

	/**
	 * Publishes in flight when a client closes are sent again and completed by the next client
	 * with the same {@link MemoryPersistence}.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testOutgoingPublishesSurviveWithMemoryPersistence() throws Exception {
		MemoryPersistence persistence = new MemoryPersistence();
		assertOutgoingPublishesSurvive(persistence, () -> keys(persistence));
	}

	/**
	 * Publishes in flight when a client closes are kept in files, and sent again and completed by
	 * the next client with a {@link MqttDefaultFilePersistence} on the same directory; the files
	 * are removed once they complete.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testOutgoingPublishesSurviveWithFilePersistence() throws Exception {
		File dir = folder.newFolder();
		assertOutgoingPublishesSurvive(new MqttDefaultFilePersistence(dir.getPath()), () -> {
			List<String> files = new ArrayList<>();
			for (File clientDir : dir.listFiles()) {
				for (File f : clientDir.listFiles()) {
					files.add(f.getName());
				}
			}
			return files;
		}, new MqttDefaultFilePersistence(dir.getPath()));
	}

	/**
	 * Publishes in flight when a client closes are kept in a SQLite database by
	 * {@link MqttDbPersistence}, and sent again and completed by the next client, which opens a
	 * new connection to the same database file, as after a restart; the rows are removed once
	 * they complete.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testOutgoingPublishesSurviveWithSqlitePersistence() throws Exception {
		String dbFile = new File(folder.newFolder(), "mqtt.db").getPath();
		JdbcSqliteBridge first = new JdbcSqliteBridge(dbFile);
		JdbcSqliteBridge second = new JdbcSqliteBridge(dbFile);
		JdbcSqliteBridge inspect = new JdbcSqliteBridge(dbFile);
		try {
			assertOutgoingPublishesSurvive(new MqttDbPersistence(first), () -> {
				List<String> keys = new ArrayList<>();
				try (SqliteCursorBridge c = inspect.rawQuery("SELECT pkey FROM mqtt_persistence", null)) {
					while (c.moveToNext()) {
						keys.add(c.getString(0));
					}
				}
				return keys;
			}, new MqttDbPersistence(second));
		} finally {
			first.close();
			second.close();
			inspect.close();
		}
	}

	/**
	 * A clean session clears what the store holds.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testCleanSessionClearsTheStore() throws Exception {
		MemoryPersistence persistence = new MemoryPersistence();
		MqttAsyncClient first = new MqttAsyncClient(sessionOptions(relayURI), persistence);
		try {
			first.connect().waitForCompletion(10_000);
			first.publish(topic(), new byte[]{1}, 1, false);
			Thread.sleep(300);
		} finally {
			first.close(true);
		}
		Assert.assertFalse("the publish should be stored", keys(persistence).isEmpty());

		MqttConnectOptions clean = sessionOptions(broker.getServerURI());
		clean.setCleanSession(true);
		MqttAsyncClient second = new MqttAsyncClient(clean, persistence);
		try {
			second.connect().waitForCompletion(10_000);
			Assert.assertTrue("a clean session should clear the store", keys(persistence).isEmpty());
			Assert.assertEquals(0, second.getPendingDeliveryTokens().length);
		} finally {
			TestClients.shutdownQuietly(second);
		}
	}

	/**
	 * An incoming QoS 2 message whose PUBREL had not arrived when the subscriber closed is
	 * delivered by the next subscriber with the same store, when the broker sends the PUBREL again.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testIncomingQos2SurvivesWithPersistence() throws Exception {
		MemoryPersistence persistence = new MemoryPersistence();
		MqttAsyncClient first = new MqttAsyncClient(sessionOptions(relayURI), persistence);
		MqttAsyncClient publisher = TestClients.createMqttAsyncClient(broker.getServerURI(), name.getMethodName() + "-pub");
		try {
			first.connect().waitForCompletion(10_000);
			first.subscribe(topic(), 2).waitForCompletion(10_000);
			publisher.connect().waitForCompletion(5000);
			publisher.publish(topic(), new byte[]{9}, 2, false).waitForCompletion(5000);
			long deadline = System.currentTimeMillis() + 5000;
			while (first.getQos2PendingIncomingPublishes().isEmpty() && System.currentTimeMillis() < deadline) {
				Thread.sleep(5);
			}
			Assert.assertFalse("the PUBLISH did not arrive", first.getQos2PendingIncomingPublishes().isEmpty());
		} finally {
			first.close(true);   // before the broker's PUBREL gets through the relay
			TestClients.shutdownQuietly(publisher);
		}
		Assert.assertTrue("the incoming message should be stored", keys(persistence).stream().anyMatch(k -> k.startsWith("r-")));

		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttAsyncClient second = new MqttAsyncClient(sessionOptions(broker.getServerURI()), persistence);
		try {
			second.setCallback(receiver);
			second.connect().waitForCompletion(10_000);
			Assert.assertTrue("message not delivered after the restart", receiver.validateReceipt(topic(), 2, new byte[]{9}));
			Assert.assertNull("delivered twice", receiver.receiveNext(1000));
			Assert.assertTrue(keys(persistence).isEmpty());
		} finally {
			TestClients.shutdownQuietly(second);
		}
	}

	/** Lists what a store holds. */
	private interface Listing {
		List<String> list() throws Exception;
	}

	/**
	 * Runs the outgoing scenario with one store instance for both clients.
	 *
	 * @param persistence the store
	 * @param listing     lists what it holds
	 * @throws Exception if the test fails unexpectedly
	 */
	private void assertOutgoingPublishesSurvive(MqttClientPersistence persistence, Listing listing) throws Exception {
		assertOutgoingPublishesSurvive(persistence, listing, persistence);
	}

	/**
	 * Publishes three QoS 1 messages and one QoS 2 message through the relay, closes the client
	 * while they are in flight, then connects a second client with the same ID and checks that
	 * it sends them again and completes them, and that the subscriber receives each.
	 *
	 * @param firstStore  the first client's store
	 * @param listing     lists what the store holds
	 * @param secondStore the second client's store, the same state as the first's
	 * @throws Exception if the test fails unexpectedly
	 */
	private void assertOutgoingPublishesSurvive(MqttClientPersistence firstStore, Listing listing,
												MqttClientPersistence secondStore) throws Exception {
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttAsyncClient subscriber = TestClients.createMqttAsyncClient(broker.getServerURI(), name.getMethodName() + "-sub");
		try {
			subscriber.setCallback(receiver);
			subscriber.connect().waitForCompletion(5000);
			subscriber.subscribe(topic(), 2).waitForCompletion(5000);

			MqttAsyncClient first = new MqttAsyncClient(sessionOptions(relayURI), firstStore);
			try {
				first.connect().waitForCompletion(10_000);
				for (int i = 1; i <= 3; i++) {
					first.publish(topic(), new byte[]{(byte) i}, 1, false);
				}
				first.publish(topic(), new byte[]{4}, 2, false);
				Thread.sleep(300);   // sent; the acknowledgements are held back by the relay
			} finally {
				first.close(true);
			}
			Assert.assertEquals("all four publishes should be stored", 4, listing.list().size());

			MqttAsyncClient second = new MqttAsyncClient(sessionOptions(relayURI), secondStore);
			try {
				second.connect().waitForCompletion(10_000);
				IMqttDeliveryToken[] restored = second.getPendingDeliveryTokens();
				Assert.assertEquals("the stored publishes should be pending", 4, restored.length);
				for (IMqttDeliveryToken token : restored) {
					token.waitForCompletion(15_000);
				}
				Assert.assertTrue("completed publishes should be removed: " + listing.list(), listing.list().isEmpty());
			} finally {
				TestClients.shutdownQuietly(second);
			}

			// QoS 1 may arrive twice (the broker had the first copies); QoS 2 exactly once.
			Set<Integer> received = new HashSet<>();
			int qos2 = 0;
			MqttV3Receiver.ReceivedMessage m;
			while ((m = receiver.receiveNext(1500)) != null) {
				int value = m.message.getPayload()[0];
				received.add(value);
				if (value == 4) {
					qos2++;
				}
			}
			Assert.assertEquals(new HashSet<>(java.util.Arrays.asList(1, 2, 3, 4)), received);
			Assert.assertEquals("the QoS 2 message should arrive once", 1, qos2);
		} finally {
			TestClients.shutdownQuietly(subscriber);
		}
	}

	/**
	 * Returns the options for this test's client, with {@code cleanSession=false}.
	 *
	 * @param serverURI the broker or relay
	 * @return the options
	 */
	private MqttConnectOptions sessionOptions(URI serverURI) {
		MqttConnectOptions options = TestClients.options(serverURI, name.getMethodName());
		options.setCleanSession(false);
		options.setConnectionTimeout(10);
		return options;
	}

	/**
	 * Returns the topic for the running test.
	 *
	 * @return the topic
	 */
	private String topic() {
		return "PersistenceTest/" + name.getMethodName() + "/" + RUN;
	}

	/** Keeps topics apart between runs against the same broker. */
	private static final String RUN = UUID.randomUUID().toString();

	/**
	 * Lists the keys in a memory store.
	 *
	 * @param persistence the store
	 * @return the keys
	 * @throws MqttPersistenceException never, for a memory store
	 */
	private static List<String> keys(MqttClientPersistence persistence) throws MqttPersistenceException {
		return Collections.list(persistence.keys());
	}
}
