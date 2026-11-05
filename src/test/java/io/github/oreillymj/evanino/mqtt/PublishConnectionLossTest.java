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
import org.junit.rules.TestName;

import java.net.URI;
import java.util.UUID;

/**
 * Every QoS 1 and 2 publish has an outcome when the connection drops while it is in flight:
 * with a clean session its token fails; with {@code cleanSession=false} it is sent again after
 * reconnecting (PUBLISH with DUP, or PUBREL) and completes, and incoming QoS 2 messages awaiting
 * PUBREL are kept so they are still delivered. The client connects through a relay
 * that holds the broker's replies back for {@value #REPLY_DELAY_MILLIS} ms, so a publish is
 * still waiting for its acknowledgement when the relay drops the connection. Not part of Paho's
 * suite. Requires a broker; see {@link MosquittoBroker}.
 */
public class PublishConnectionLossTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID and topic. */
	@Rule
	public final TestName name = new TestName();

	/** How long the relay holds back each reply from the broker. */
	private static final long REPLY_DELAY_MILLIS = 500;

	private static DelayingRelay relay;
	private static URI relayURI;
	private static String topicPrefix;

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
		topicPrefix = "PublishConnectionLossTest-" + UUID.randomUUID() + "-";
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
	 * A QoS 1 publish whose PUBACK is lost with the connection fails with
	 * {@link MqttException#REASON_CODE_CONNECTION_LOST}.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testQos1InFlightFailsOnConnectionLoss() throws Exception {
		assertInFlightFailsOnConnectionLoss(1);
	}

	/**
	 * A QoS 2 publish whose PUBREC is lost with the connection fails with
	 * {@link MqttException#REASON_CODE_CONNECTION_LOST}.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testQos2InFlightFailsOnConnectionLoss() throws Exception {
		assertInFlightFailsOnConnectionLoss(2);
	}

	/**
	 * A subscribe whose SUBACK is lost with the connection fails with
	 * {@link MqttException#REASON_CODE_CONNECTION_LOST}.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSubscribeInFlightFailsOnConnectionLoss() throws Exception {
		MqttAsyncClient client = new MqttAsyncClient(TestClients.options(relayURI, name.getMethodName()));
		try {
			client.connect().waitForCompletion(5000);
			IMqttToken token = client.subscribe(topic(), 1);
			Thread.sleep(100);
			relay.dropConnections();

			try {
				token.waitForCompletion(5000);
				Assert.fail("the subscribe should have failed");
			} catch (MqttException e) {
				Assert.assertEquals(MqttException.REASON_CODE_CONNECTION_LOST, e.getReasonCode());
			}
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * With a clean session and automatic reconnect, QoS 1 publishes made steadily while the
	 * connection is dropped again and again each get exactly one outcome: those in flight at a
	 * drop fail, and those made while disconnected are held and completed after the reconnect.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testEveryPublishHasOneOutcomeAcrossReconnects() throws Exception {
		assertOneOutcomeEachAcrossReconnects(true);
	}

	/**
	 * With {@code cleanSession=false} and automatic reconnect, QoS 1 publishes made steadily
	 * while the connection is dropped again and again all complete: those in flight at a drop
	 * are sent again after the reconnect, and none fails.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testEveryPublishCompletesAcrossReconnectsWithSession() throws Exception {
		assertOneOutcomeEachAcrossReconnects(false);
	}

	/**
	 * Publishes steadily for 4 s while dropping the connection three times, then checks that
	 * every publish has exactly one outcome: with a clean session some fail with
	 * {@link MqttException#REASON_CODE_CONNECTION_LOST}; with a kept session none fails.
	 *
	 * @param cleanSession the session option
	 * @throws Exception if the test fails unexpectedly
	 */
	private void assertOneOutcomeEachAcrossReconnects(boolean cleanSession) throws Exception {
		MqttConnectOptions options = TestClients.options(relayURI, name.getMethodName());
		options.setCleanSession(cleanSession);
		options.setAutomaticReconnect(true);
		options.setMaxRetries(-1);
		options.setMaxInflight(MqttConnectOptions.MAX_INFLIGHT_LIMIT);   // publishes pile up while disconnected
		MqttAsyncClient client = new MqttAsyncClient(options);
		try {
			client.connect().waitForCompletion(5000);
			java.util.List<Outcome> outcomes = new java.util.ArrayList<>();
			long end = System.currentTimeMillis() + 4000;
			long nextDrop = System.currentTimeMillis() + 700;
			while (System.currentTimeMillis() < end) {
				Outcome outcome = new Outcome();
				client.publish(topic(), new byte[]{1}, 1, false, null, outcome);
				outcomes.add(outcome);
				if (System.currentTimeMillis() >= nextDrop) {
					relay.dropConnections();
					nextDrop += 1300;
				}
				Thread.sleep(20);
			}

			int completed = 0;
			int failed = 0;
			for (Outcome outcome : outcomes) {
				Assert.assertTrue("a publish had no outcome", outcome.await(20_000));
				if (outcome.failure == null) {
					completed++;
				} else {
					failed++;
					Assert.assertEquals(MqttException.REASON_CODE_CONNECTION_LOST,
							((MqttException) outcome.failure).getReasonCode());
				}
			}
			Thread.sleep(200);
			for (Outcome outcome : outcomes) {
				Assert.assertEquals("a publish had more than one outcome", 1, outcome.calls.get());
			}
			if (cleanSession) {
				Assert.assertTrue("publishes should have failed at the drops", failed > 0);
			} else {
				Assert.assertEquals("no publish should fail with a kept session", 0, failed);
			}
			Assert.assertTrue("publishes should have completed", completed > 0);
			Assert.assertEquals("every slot should be free again", 0, client.getPendingPublishes().size());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * With {@code cleanSession=false}, a QoS 1 publish whose PUBACK is lost with the connection
	 * is sent again after the automatic reconnect and completes; the subscriber receives it.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testQos1InFlightIsResentAfterReconnect() throws Exception {
		try (Subscriber subscriber = new Subscriber(topic(), 1)) {
			MqttAsyncClient client = connectWithSession();
			try {
				Outcome outcome = new Outcome();
				client.publish(topic(), new byte[]{1}, 1, false, null, outcome);
				Thread.sleep(100);
				relay.dropConnections();

				Assert.assertTrue("the publish did not complete", outcome.await(15_000));
				Assert.assertNull("the publish should have completed", outcome.failure);
				// The broker had the first copy, so at QoS 1 the subscriber may get it twice.
				Assert.assertTrue(subscriber.receiver.validateReceipt(topic(), 1, new byte[]{1}));
				Assert.assertEquals(0, client.getPendingPublishes().size());
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * With {@code cleanSession=false}, a QoS 2 publish whose PUBREC is lost with the connection
	 * is sent again, as a duplicate with the same packet identifier, after the reconnect: it
	 * completes and the subscriber receives it once.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testQos2PublishIsResentAfterReconnect() throws Exception {
		try (Subscriber subscriber = new Subscriber(topic(), 2)) {
			MqttAsyncClient client = connectWithSession();
			try {
				Outcome outcome = new Outcome();
				client.publish(topic(), new byte[]{2}, 2, false, null, outcome);
				Thread.sleep(100);
				relay.dropConnections();

				Assert.assertTrue("the publish did not complete", outcome.await(15_000));
				Assert.assertNull("the publish should have completed", outcome.failure);
				Assert.assertTrue(subscriber.receiver.validateReceipt(topic(), 2, new byte[]{2}));
				Assert.assertNull("delivered twice", subscriber.receiver.receiveNext(1000));
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * With {@code cleanSession=false}, a QoS 2 publish whose PUBREC has arrived but whose
	 * PUBCOMP is lost with the connection has its PUBREL sent again after the reconnect: it
	 * completes and the subscriber receives it once.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testQos2PubrelIsResentAfterReconnect() throws Exception {
		try (Subscriber subscriber = new Subscriber(topic(), 2)) {
			MqttAsyncClient client = connectWithSession();
			try {
				Outcome outcome = new Outcome();
				IMqttDeliveryToken token = client.publish(topic(), new byte[]{3}, 2, false, null, outcome);
				MqttPendingPublish pending = client.getPendingPublishes().get(token.getMessageId());
				long deadline = System.currentTimeMillis() + 5000;
				while (pending.getPubrelMessage() == null && System.currentTimeMillis() < deadline) {
					Thread.sleep(5);
				}
				Assert.assertNotNull("PUBREC did not arrive", pending.getPubrelMessage());
				relay.dropConnections();   // PUBCOMP is still held back by the relay

				Assert.assertTrue("the publish did not complete", outcome.await(15_000));
				Assert.assertNull("the publish should have completed", outcome.failure);
				Assert.assertTrue(subscriber.receiver.validateReceipt(topic(), 2, new byte[]{3}));
				Assert.assertNull("delivered twice", subscriber.receiver.receiveNext(1000));
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * With {@code cleanSession=false}, an incoming QoS 2 message whose PUBREL is lost with the
	 * connection is kept, so the PUBREL the broker sends again after the reconnect delivers it,
	 * once.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testIncomingQos2IsDeliveredAfterReconnect() throws Exception {
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttConnectOptions options = TestClients.options(relayURI, name.getMethodName());
		options.setCleanSession(false);
		options.setAutomaticReconnect(true);
		options.setMaxRetries(-1);
		MqttAsyncClient client = new MqttAsyncClient(options);
		MqttAsyncClient publisher = TestClients.createMqttAsyncClient(broker.getServerURI(), name.getMethodName() + "-pub");
		try {
			client.setCallback(receiver);
			client.connect().waitForCompletion(5000);
			client.subscribe(topic(), 2).waitForCompletion(5000);
			publisher.connect().waitForCompletion(5000);

			publisher.publish(topic(), new byte[]{4}, 2, false).waitForCompletion(5000);
			long deadline = System.currentTimeMillis() + 5000;
			while (client.getQos2PendingIncomingPublishes().isEmpty() && System.currentTimeMillis() < deadline) {
				Thread.sleep(5);
			}
			Assert.assertFalse("the PUBLISH did not arrive", client.getQos2PendingIncomingPublishes().isEmpty());
			relay.dropConnections();   // the broker's PUBREL is still held back by the relay

			Assert.assertTrue("message not delivered after the reconnect", receiver.validateReceipt(topic(), 2, new byte[]{4}));
			Assert.assertNull("delivered twice", receiver.receiveNext(1000));
		} finally {
			TestClients.shutdownQuietly(publisher);
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * With {@code cleanSession=false}, a publish left in flight by {@code disconnect()} stays
	 * pending, as in Paho, and completes after the next {@code connect()}.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testDisconnectKeepsSessionPublishForNextConnect() throws Exception {
		MqttConnectOptions options = TestClients.options(relayURI, name.getMethodName());
		options.setCleanSession(false);
		MqttAsyncClient client = new MqttAsyncClient(options);
		try {
			client.connect().waitForCompletion(5000);
			Outcome outcome = new Outcome();
			client.publish(topic(), new byte[]{6}, 1, false, null, outcome);
			client.disconnect().waitForCompletion(5000);
			Assert.assertFalse("the publish should still be pending", outcome.await(1000));

			client.connect().waitForCompletion(5000);
			Assert.assertTrue("the publish did not complete after connecting", outcome.await(5000));
			Assert.assertNull("the publish should have completed", outcome.failure);
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Publishes still pending when the client is closed, here one held because the client was
	 * never connected, fail with {@link MqttException#REASON_CODE_CLIENT_CLOSED}.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testCloseFailsPendingPublishes() throws Exception {
		MqttAsyncClient client = new MqttAsyncClient(TestClients.options(relayURI, name.getMethodName()));
		IMqttDeliveryToken token = client.publish(topic(), new byte[]{5}, 1, false);
		client.close(true);
		try {
			token.waitForCompletion(2000);
			Assert.fail("the publish should have failed");
		} catch (MqttException e) {
			Assert.assertEquals(MqttException.REASON_CODE_CLIENT_CLOSED, e.getReasonCode());
		}
	}

	/**
	 * Connects through the relay with {@code cleanSession=false} and automatic reconnect,
	 * starting from an empty session.
	 *
	 * @return the connected client
	 * @throws Exception if the connection fails
	 */
	private MqttAsyncClient connectWithSession() throws Exception {
		MqttConnectOptions options = TestClients.options(relayURI, name.getMethodName());
		options.setCleanSession(false);
		options.setAutomaticReconnect(true);
		options.setMaxRetries(-1);
		MqttAsyncClient client = new MqttAsyncClient(options);
		client.connect().waitForCompletion(5000);
		return client;
	}

	/**
	 * A client connected directly to the broker and subscribed to a topic.
	 */
	private final class Subscriber implements AutoCloseable {
		final MqttV3Receiver receiver = new MqttV3Receiver();
		private final MqttAsyncClient client;

		/**
		 * Connects and subscribes.
		 *
		 * @param topic the topic
		 * @param qos   the subscription QoS
		 * @throws Exception if the connection fails
		 */
		Subscriber(String topic, int qos) throws Exception {
			client = TestClients.createMqttAsyncClient(broker.getServerURI(), name.getMethodName() + "-sub");
			client.setCallback(receiver);
			client.connect().waitForCompletion(5000);
			client.subscribe(topic, qos).waitForCompletion(5000);
		}

		/**
		 * Disconnects the subscriber.
		 */
		@Override
		public void close() {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Publishes made just before {@code disconnect()} are written before DISCONNECT, so the
	 * broker delivers them; the QoS 0 publish completes.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testPublishThenDisconnectIsDelivered() throws Exception {
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttAsyncClient subscriber = TestClients.createMqttAsyncClient(broker.getServerURI(), name.getMethodName() + "-sub");
		MqttAsyncClient client = TestClients.createMqttAsyncClient(broker.getServerURI(), name.getMethodName());
		try {
			subscriber.setCallback(receiver);
			subscriber.connect().waitForCompletion(5000);
			subscriber.subscribe(topic(), 1).waitForCompletion(5000);
			client.connect().waitForCompletion(5000);

			IMqttDeliveryToken qos0 = client.publish(topic(), new byte[]{0}, 0, false);
			client.publish(topic(), new byte[]{1}, 1, false);
			client.disconnect().waitForCompletion(5000);

			qos0.waitForCompletion(1000);
			// MQTT orders messages within QoS 1 and 2, not between QoS 0 and QoS 1, and the
			// broker may deliver these two either way round.
			java.util.Set<Integer> received = new java.util.HashSet<>();
			for (int i = 0; i < 2; i++) {
				MqttV3Receiver.ReceivedMessage m = receiver.receiveNext(5000);
				Assert.assertNotNull("a publish was not delivered (got " + received + ")", m);
				Assert.assertEquals(m.message.getPayload()[0], m.message.getQos());
				received.add(m.message.getQos());
			}
			Assert.assertEquals(new java.util.HashSet<>(java.util.Arrays.asList(0, 1)), received);
		} finally {
			TestClients.shutdownQuietly(client);
			TestClients.shutdownQuietly(subscriber);
		}
	}

	/**
	 * Publishes, drops the connection before the acknowledgement arrives, and checks that the
	 * token and its listener report the loss.
	 *
	 * @param qos the QoS to publish at
	 * @throws Exception if the test fails unexpectedly
	 */
	private void assertInFlightFailsOnConnectionLoss(int qos) throws Exception {
		MqttAsyncClient client = new MqttAsyncClient(TestClients.options(relayURI, name.getMethodName()));
		try {
			client.connect().waitForCompletion(5000);
			Outcome outcome = new Outcome();
			IMqttDeliveryToken token = client.publish(topic(), new byte[]{1}, qos, false, null, outcome);
			Thread.sleep(100);
			relay.dropConnections();

			Assert.assertTrue("the listener was not called", outcome.await(5000));
			Assert.assertNotNull("the publish should have failed", outcome.failure);
			Assert.assertTrue(token.isComplete());
			Assert.assertNotNull(token.getException());
			Assert.assertEquals(MqttException.REASON_CODE_CONNECTION_LOST,
					((MqttException) token.getException()).getReasonCode());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Returns the topic for the running test.
	 *
	 * @return the topic
	 */
	private String topic() {
		return topicPrefix + name.getMethodName();
	}

	/**
	 * Records the first outcome reported to a publish listener, and how many were reported.
	 */
	private static final class Outcome implements IMqttActionListener {
		private final java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
		private final java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
		private volatile Throwable failure;

		@Override
		public void onSuccess(IMqttToken asyncActionToken) {
			calls.incrementAndGet();
			done.countDown();
		}

		@Override
		public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
			if (calls.getAndIncrement() == 0) {
				failure = exception;
			}
			done.countDown();
		}

		/**
		 * Waits for an outcome.
		 *
		 * @param millis how long to wait
		 * @return {@code true} if an outcome was reported in time
		 * @throws InterruptedException if interrupted
		 */
		boolean await(long millis) throws InterruptedException {
			return done.await(millis, java.util.concurrent.TimeUnit.MILLISECONDS);
		}
	}
}
