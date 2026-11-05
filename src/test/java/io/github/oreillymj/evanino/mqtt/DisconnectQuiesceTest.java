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
 * {@code disconnect(quiesceTimeout)}, the {@code disconnectForcibly} timeouts and
 * {@code close(force)}: in-flight
 * messages are given up to the quiesce timeout to complete before DISCONNECT is sent, and new
 * work is refused meanwhile. The client connects through a
 * relay that holds the broker's replies back for {@value #REPLY_DELAY_MILLIS} ms, so a QoS 1
 * publish stays in flight long enough to see the difference. Not part of Paho's suite. Requires
 * a broker; see {@link MosquittoBroker}.
 */
public class DisconnectQuiesceTest {

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
		topicPrefix = "DisconnectQuiesceTest-" + UUID.randomUUID() + "-";
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
	 * With a quiesce timeout longer than the PUBACK takes, the publish completes before the
	 * disconnect does.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testQuiesceWaitsForInFlightPublish() throws Exception {
		MqttAsyncClient client = connect();
		try {
			IMqttDeliveryToken publish = TestClients.publish(client, topic(), new byte[]{1}, 1, false);
			long start = System.currentTimeMillis();
			IMqttToken disconnect = client.disconnect(5000);
			Assert.assertFalse("disconnect must not block", System.currentTimeMillis() - start > 200);
			disconnect.waitForCompletion(10_000);
			long took = System.currentTimeMillis() - start;

			Assert.assertTrue("publish was not acknowledged before DISCONNECT", publish.isComplete());
			Assert.assertNull(publish.getException());
			Assert.assertTrue("disconnect should have waited for the PUBACK (" + took + " ms)", took >= REPLY_DELAY_MILLIS - 100);
			Assert.assertTrue("disconnect should not wait the whole quiesce timeout (" + took + " ms)", took < 4000);
			Assert.assertFalse(client.isConnected());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * With no quiesce timeout, DISCONNECT is sent at once and the in-flight publish fails
	 * with {@code REASON_CODE_CLIENT_DISCONNECTING} when the connection closes.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testNoQuiesceDisconnectsAtOnce() throws Exception {
		MqttAsyncClient client = connect();
		try {
			IMqttDeliveryToken publish = TestClients.publish(client, topic(), new byte[]{1}, 1, false);
			long start = System.currentTimeMillis();
			client.disconnect().waitForCompletion(5000);
			long took = System.currentTimeMillis() - start;
			Assert.assertTrue("disconnect() should not wait (" + took + " ms)", took < REPLY_DELAY_MILLIS - 100);
			assertEndedByDisconnect(publish);
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * A quiesce timeout shorter than the PUBACK takes ends the wait at the timeout.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testQuiesceStopsAtTimeout() throws Exception {
		MqttAsyncClient client = connect();
		try {
			IMqttDeliveryToken publish = TestClients.publish(client, topic(), new byte[]{1}, 1, false);
			long start = System.currentTimeMillis();
			client.disconnect(150).waitForCompletion(5000);
			long took = System.currentTimeMillis() - start;
			Assert.assertTrue("should wait about the quiesce timeout (" + took + " ms)", took >= 100 && took < REPLY_DELAY_MILLIS - 50);
			assertEndedByDisconnect(publish);
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * While quiescing, new publishes, subscribes and unsubscribes are refused with
	 * {@code REASON_CODE_CLIENT_DISCONNECTING}; after the disconnect, the client can connect
	 * and publish again.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testNewWorkRefusedWhileQuiescing() throws Exception {
		MqttAsyncClient client = connect();
		try {
			TestClients.publish(client, topic(), new byte[]{1}, 1, false);
			IMqttToken disconnect = client.disconnect(5000);

			assertDisconnecting(() -> TestClients.publish(client, topic(), new byte[]{2}, 1, false));
			assertDisconnecting(() -> TestClients.publish(client, topic(), new byte[]{3}, 0, false));
			assertDisconnecting(() -> client.subscribe(topic(), 1));
			assertDisconnecting(() -> client.subscribe(new String[]{topic()}, new int[]{1}));
			assertDisconnecting(() -> client.unsubscribe(topic()));
			assertDisconnecting(() -> client.unsubscribe(new String[]{topic()}));

			disconnect.waitForCompletion(10_000);
			client.connect().waitForCompletion(5000);
			TestClients.publish(client, topic(), new byte[]{4}, 1, false).waitForCompletion(5000);
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * The blocking client's {@code disconnect(quiesceTimeout)} also waits for the in-flight
	 * publish, and does not time out while it waits.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testBlockingDisconnectQuiesces() throws Exception {
		MqttClient client = new MqttClient(relayURI.toString(), name.getMethodName());
		try {
			client.connect();
			IMqttDeliveryToken publish = client.getAsyncClient().publish(topic(), new byte[]{1}, 1, false);
			client.disconnect(5000);
			Assert.assertTrue("publish was not acknowledged before DISCONNECT", publish.isComplete());
			Assert.assertFalse(client.isConnected());
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * {@code disconnectForcibly(quiesceTimeout, disconnectTimeout)} works as in Paho: it waits
	 * for the in-flight publish, then sends DISCONNECT, so the broker discards the will.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testForciblyQuiescesAndSendsDisconnect() throws Exception {
		try (WillWatcher watcher = new WillWatcher(topic() + "/will")) {
			MqttAsyncClient client = connectWithWill(watcher.topic);
			try {
				IMqttDeliveryToken publish = TestClients.publish(client, topic(), new byte[]{1}, 1, false);
				client.disconnectForcibly(5000, 1000).waitForCompletion(10_000);
				Assert.assertTrue("publish was not acknowledged before DISCONNECT", publish.isComplete());
				Assert.assertFalse(client.isConnected());
				Assert.assertNull("will published although DISCONNECT was sent", watcher.receiver.receiveNext(1500));
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * {@code disconnectForcibly(disconnectTimeout)} sends DISCONNECT without quiescing: the
	 * in-flight publish fails unacknowledged, and the broker discards the will.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testForciblyWithDisconnectTimeoutSendsDisconnect() throws Exception {
		try (WillWatcher watcher = new WillWatcher(topic() + "/will")) {
			MqttAsyncClient client = connectWithWill(watcher.topic);
			try {
				IMqttDeliveryToken publish = TestClients.publish(client, topic(), new byte[]{1}, 1, false);
				client.disconnectForcibly(1000).waitForCompletion(5000);
				assertEndedByDisconnect(publish);
				Assert.assertNull("will published although DISCONNECT was sent", watcher.receiver.receiveNext(1500));
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * {@code disconnectForcibly()} closes at once without DISCONNECT, so the broker publishes
	 * the will.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testForciblyWithoutArgumentsClosesAtOnce() throws Exception {
		try (WillWatcher watcher = new WillWatcher(topic() + "/will")) {
			MqttAsyncClient client = connectWithWill(watcher.topic);
			try {
				TestClients.publish(client, topic(), new byte[]{1}, 1, false);
				IMqttToken token = client.disconnectForcibly();
				Assert.assertTrue("token should already be complete", token.isComplete());
				Assert.assertTrue("will not published", watcher.receiver.validateReceipt(watcher.topic, 1, WILL));
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * {@code disconnectForcibly(quiesce, disconnect, false)} quiesces for at most the quiesce
	 * timeout, then closes without DISCONNECT, so the broker publishes the will.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testForciblyQuiescesWithoutSendingDisconnect() throws Exception {
		try (WillWatcher watcher = new WillWatcher(topic() + "/will")) {
			MqttAsyncClient client = connectWithWill(watcher.topic);
			try {
				IMqttDeliveryToken publish = TestClients.publish(client, topic(), new byte[]{1}, 1, false);
				long start = System.currentTimeMillis();
				client.disconnectForcibly(150, 1000, false).waitForCompletion(5000);
				long took = System.currentTimeMillis() - start;
				Assert.assertTrue("should quiesce for about 150 ms (" + took + " ms)", took >= 100 && took < REPLY_DELAY_MILLIS - 50);
				assertEndedByDisconnect(publish);
				Assert.assertTrue("will not published", watcher.receiver.validateReceipt(watcher.topic, 1, WILL));
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * {@code disconnect()} sends DISCONNECT before closing, so the broker discards the will.
	 * It used to close the channel first, so the will was always published.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testDisconnectSendsDisconnect() throws Exception {
		try (WillWatcher watcher = new WillWatcher(topic() + "/will")) {
			MqttAsyncClient client = connectWithWill(watcher.topic);
			try {
				client.disconnect().waitForCompletion(5000);
				Assert.assertFalse(client.isConnected());
				Assert.assertNull("will published although DISCONNECT should have been sent", watcher.receiver.receiveNext(1500));
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * {@code close(true)} closes the connection at once without DISCONNECT, so the broker
	 * publishes the will, and releases the client.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testCloseForcedSkipsDisconnect() throws Exception {
		try (WillWatcher watcher = new WillWatcher(topic() + "/will")) {
			MqttClient client = blockingClientWithWill(watcher.topic);
			IMqttDeliveryToken publish = client.getAsyncClient().publish(topic(), new byte[]{1}, 1, false);
			long start = System.currentTimeMillis();
			client.close(true);
			long took = System.currentTimeMillis() - start;
			Assert.assertTrue("close(true) should not wait (" + took + " ms)", took < REPLY_DELAY_MILLIS - 100);
			Assert.assertFalse(client.isConnected());
			assertEndedByClose(publish);
			Assert.assertTrue("will not published", watcher.receiver.validateReceipt(watcher.topic, 1, WILL));
		}
	}

	/**
	 * {@code close(false)}, like {@code close()}, disconnects cleanly first, so the broker
	 * discards the will.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testCloseUnforcedDisconnectsCleanly() throws Exception {
		for (boolean viaCloseWithFlag : new boolean[]{true, false}) {
			try (WillWatcher watcher = new WillWatcher(topic() + "/will" + viaCloseWithFlag)) {
				MqttClient client = blockingClientWithWill(watcher.topic);
				if (viaCloseWithFlag) {
					client.close(false);
				} else {
					client.close();
				}
				Assert.assertFalse(client.isConnected());
				Assert.assertNull("will published although DISCONNECT was sent", watcher.receiver.receiveNext(1500));
			}
		}
	}

	/**
	 * The asynchronous client's {@code close(true)}, called through {@link IMqttAsyncClient}
	 * as Paho code would, closes at once without DISCONNECT, so the broker publishes the will.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testAsyncCloseForcedSkipsDisconnect() throws Exception {
		try (WillWatcher watcher = new WillWatcher(topic() + "/will")) {
			IMqttAsyncClient client = connectWithWill(watcher.topic);
			long start = System.currentTimeMillis();
			client.close(true);
			long took = System.currentTimeMillis() - start;
			Assert.assertTrue("close(true) should not wait (" + took + " ms)", took < REPLY_DELAY_MILLIS - 100);
			Assert.assertFalse(client.isConnected());
			Assert.assertTrue("will not published", watcher.receiver.validateReceipt(watcher.topic, 1, WILL));
		}
	}

	/**
	 * The asynchronous client's {@code close()} and {@code close(false)} disconnect cleanly
	 * first, so the broker discards the will.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testAsyncCloseUnforcedDisconnectsCleanly() throws Exception {
		for (boolean viaCloseWithFlag : new boolean[]{true, false}) {
			try (WillWatcher watcher = new WillWatcher(topic() + "/will" + viaCloseWithFlag)) {
				IMqttAsyncClient client = connectWithWill(watcher.topic);
				if (viaCloseWithFlag) {
					client.close(false);
				} else {
					client.close();
				}
				Assert.assertFalse(client.isConnected());
				Assert.assertNull("will published although DISCONNECT was sent", watcher.receiver.receiveNext(1500));
			}
		}
	}

	/**
	 * If the connection drops while quiescing, the disconnect still completes. It used to hang:
	 * DISCONNECT could not be sent on the closed channel and the token was never completed.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testConnectionLostWhileQuiescing() throws Exception {
		MqttAsyncClient client = connect();
		try {
			TestClients.publish(client, topic(), new byte[]{1}, 1, false);
			long start = System.currentTimeMillis();
			IMqttToken disconnect = client.disconnect(10_000);
			Thread.sleep(100);
			relay.dropConnections();
			disconnect.waitForCompletion(5000);
			long took = System.currentTimeMillis() - start;
			Assert.assertTrue("disconnect should end when the connection drops (" + took + " ms)", took < 5000);
			Assert.assertFalse(client.isConnected());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * In Paho mode, {@code disconnect()} and {@code disconnect(userContext, callback)} quiesce
	 * as Paho's do: they wait for the in-flight publish, well within the 30 s limit.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testPahoModeDisconnectQuiesces() throws Exception {
		for (boolean withListener : new boolean[]{false, true}) {
			MqttAsyncClient client = connectInPahoMode();
			try {
				IMqttDeliveryToken publish = TestClients.publish(client, topic(), new byte[]{1}, 1, false);
				long start = System.currentTimeMillis();
				IMqttToken disconnect = withListener ? client.disconnect(null, null) : client.disconnect();
				disconnect.waitForCompletion(10_000);
				long took = System.currentTimeMillis() - start;
				Assert.assertTrue("publish was not acknowledged before DISCONNECT", publish.isComplete());
				Assert.assertTrue("disconnect should have waited for the PUBACK (" + took + " ms)", took >= REPLY_DELAY_MILLIS - 100);
				Assert.assertFalse(client.isConnected());
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * In Paho mode the blocking client's {@code disconnect()} quiesces too, and does not time
	 * out while it waits.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testPahoModeBlockingDisconnectQuiesces() throws Exception {
		MqttConnectOptions options = TestClients.options(relayURI, name.getMethodName());
		options.enablePahoMode(true);
		MqttClient client = new MqttClient(options);
		client.setTimeToWait(200);   // shorter than the PUBACK takes: only the quiesce time covers it
		try {
			client.getAsyncClient().connect().waitForCompletion(5000);
			IMqttDeliveryToken publish = client.getAsyncClient().publish(topic(), new byte[]{1}, 1, false);
			client.disconnect();
			Assert.assertTrue("publish was not acknowledged before DISCONNECT", publish.isComplete());
			Assert.assertFalse(client.isConnected());
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * In Paho mode, {@code disconnectForcibly()} does what Paho's does: it quiesces, then sends
	 * DISCONNECT, so the broker discards the will. Outside Paho mode it closes at once and the
	 * will is published; see {@link #testForciblyWithoutArgumentsClosesAtOnce()}.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testPahoModeForciblyQuiescesAndSendsDisconnect() throws Exception {
		try (WillWatcher watcher = new WillWatcher(topic() + "/will")) {
			MqttConnectOptions options = TestClients.options(relayURI, name.getMethodName());
			options.enablePahoMode(true);
			options.setWill(watcher.topic, WILL, 1, false);
			MqttAsyncClient client = new MqttAsyncClient(options);
			try {
				client.connect().waitForCompletion(5000);
				IMqttDeliveryToken publish = TestClients.publish(client, topic(), new byte[]{1}, 1, false);
				client.disconnectForcibly().waitForCompletion(10_000);
				Assert.assertTrue("publish was not acknowledged before DISCONNECT", publish.isComplete());
				Assert.assertFalse(client.isConnected());
				Assert.assertNull("will published although DISCONNECT was sent", watcher.receiver.receiveNext(1500));
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * In Paho mode, {@code disconnectForcibly(disconnectTimeout)} quiesces first, as Paho's
	 * does, on both clients.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testPahoModeForciblyWithDisconnectTimeoutQuiesces() throws Exception {
		MqttAsyncClient client = connectInPahoMode();
		try {
			IMqttDeliveryToken publish = TestClients.publish(client, topic(), new byte[]{1}, 1, false);
			client.disconnectForcibly(1000).waitForCompletion(10_000);
			Assert.assertTrue("publish was not acknowledged before DISCONNECT", publish.isComplete());
		} finally {
			TestClients.shutdownQuietly(client);
		}

		MqttConnectOptions options = TestClients.options(relayURI, name.getMethodName() + "-blocking");
		options.enablePahoMode(true);
		MqttClient blocking = new MqttClient(options);
		blocking.setTimeToWait(200);
		try {
			blocking.getAsyncClient().connect().waitForCompletion(5000);
			IMqttDeliveryToken publish = blocking.getAsyncClient().publish(topic(), new byte[]{2}, 1, false);
			blocking.disconnectForcibly(1000);
			Assert.assertTrue("publish was not acknowledged before DISCONNECT", publish.isComplete());
			Assert.assertFalse(blocking.isConnected());
		} finally {
			TestClients.closeQuietly(blocking);
		}
	}

	/**
	 * In Paho mode the blocking client's {@code disconnectForcibly()} quiesces and sends
	 * DISCONNECT, so the broker discards the will.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testPahoModeBlockingForciblyQuiesces() throws Exception {
		try (WillWatcher watcher = new WillWatcher(topic() + "/will")) {
			MqttConnectOptions options = TestClients.options(relayURI, name.getMethodName());
			options.enablePahoMode(true);
			options.setWill(watcher.topic, WILL, 1, false);
			MqttClient client = new MqttClient(options);
			client.setTimeToWait(200);
			try {
				client.getAsyncClient().connect().waitForCompletion(5000);
				IMqttDeliveryToken publish = client.getAsyncClient().publish(topic(), new byte[]{1}, 1, false);
				client.disconnectForcibly();
				Assert.assertTrue("publish was not acknowledged before DISCONNECT", publish.isComplete());
				Assert.assertNull("will published although DISCONNECT was sent", watcher.receiver.receiveNext(1500));
			} finally {
				TestClients.closeQuietly(client);
			}
		}
	}

	/**
	 * Paho mode does not change {@code close(true)}: it still closes at once without
	 * DISCONNECT, so the broker publishes the will.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testPahoModeCloseForcedStillSkipsDisconnect() throws Exception {
		try (WillWatcher watcher = new WillWatcher(topic() + "/will")) {
			MqttConnectOptions options = TestClients.options(relayURI, name.getMethodName());
			options.enablePahoMode(true);
			options.setWill(watcher.topic, WILL, 1, false);
			MqttClient client = new MqttClient(options);
			client.connect();
			IMqttDeliveryToken publish = client.getAsyncClient().publish(topic(), new byte[]{1}, 1, false);
			long start = System.currentTimeMillis();
			client.close(true);
			long took = System.currentTimeMillis() - start;
			Assert.assertTrue("close(true) should not wait (" + took + " ms)", took < REPLY_DELAY_MILLIS - 100);
			assertEndedByClose(publish);
			Assert.assertTrue("will not published", watcher.receiver.validateReceipt(watcher.topic, 1, WILL));
		}
	}

	// -------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------

	/**
	 * Connects an asynchronous client in Paho mode through the relay.
	 *
	 * @return the connected client
	 * @throws Exception if the connection fails
	 */
	private MqttAsyncClient connectInPahoMode() throws Exception {
		MqttConnectOptions options = TestClients.options(relayURI, name.getMethodName());
		options.enablePahoMode(true);
		MqttAsyncClient client = new MqttAsyncClient(options);
		client.connect().waitForCompletion(5000);
		return client;
	}

	/** An action expected to be refused. */
	private interface Action {
		void run() throws Exception;
	}

	/**
	 * Checks that a publish was not acknowledged before the disconnect, and so failed with
	 * {@link MqttException#REASON_CODE_CLIENT_DISCONNECTING} when the connection closed.
	 *
	 * @param publish the publish token
	 * @throws Exception if waiting is interrupted
	 */
	private static void assertEndedByDisconnect(IMqttDeliveryToken publish) throws Exception {
		assertEndedWith(publish, MqttException.REASON_CODE_CLIENT_DISCONNECTING);
	}

	/**
	 * Checks that a publish was not acknowledged before the client was closed, and so failed
	 * with {@link MqttException#REASON_CODE_CLIENT_CLOSED}.
	 *
	 * @param publish the publish token
	 * @throws Exception if waiting is interrupted
	 */
	private static void assertEndedByClose(IMqttDeliveryToken publish) throws Exception {
		assertEndedWith(publish, MqttException.REASON_CODE_CLIENT_CLOSED);
	}

	/**
	 * Checks that a publish failed, unacknowledged, with a reason code.
	 *
	 * @param publish    the publish token
	 * @param reasonCode the expected reason code
	 * @throws Exception if waiting is interrupted
	 */
	private static void assertEndedWith(IMqttDeliveryToken publish, int reasonCode) throws Exception {
		try {
			publish.waitForCompletion(2000);
			Assert.fail("publish cannot have been acknowledged yet");
		} catch (MqttException e) {
			Assert.assertEquals(reasonCode, e.getReasonCode());
		}
	}

	/**
	 * Checks that an action is refused because the client is disconnecting.
	 *
	 * @param action the action
	 * @throws Exception if the action fails in another way
	 */
	private static void assertDisconnecting(Action action) throws Exception {
		try {
			action.run();
			Assert.fail("action should be refused while quiescing");
		} catch (MqttException e) {
			Assert.assertEquals(MqttException.REASON_CODE_CLIENT_DISCONNECTING, e.getReasonCode());
		}
	}

	/**
	 * Returns this test's topic.
	 *
	 * @return the topic
	 */
	private String topic() {
		return topicPrefix + name.getMethodName();
	}

	/**
	 * Connects an asynchronous client through the relay. The connect takes a little over
	 * {@value #REPLY_DELAY_MILLIS} ms, as the CONNACK is delayed too.
	 *
	 * @return the connected client
	 * @throws Exception if the connection fails
	 */
	private MqttAsyncClient connect() throws Exception {
		MqttAsyncClient client = TestClients.createMqttAsyncClient(relayURI, name.getMethodName());
		client.connect().waitForCompletion(5000);
		return client;
	}

	/** The will payload used by the forced-disconnect tests. */
	private static final byte[] WILL = "gone".getBytes(java.nio.charset.StandardCharsets.UTF_8);

	/**
	 * Connects an asynchronous client through the relay with a will on the given topic.
	 *
	 * @param willTopic the will topic
	 * @return the connected client
	 * @throws Exception if the connection fails
	 */
	private MqttAsyncClient connectWithWill(String willTopic) throws Exception {
		MqttConnectOptions options = TestClients.options(relayURI, name.getMethodName());
		options.setWill(willTopic, WILL, 1, false);
		MqttAsyncClient client = new MqttAsyncClient(options);
		client.connect().waitForCompletion(5000);
		return client;
	}

	/**
	 * Connects a blocking client through the relay with a will on the given topic.
	 *
	 * @param willTopic the will topic
	 * @return the connected client
	 * @throws Exception if the connection fails
	 */
	private MqttClient blockingClientWithWill(String willTopic) throws Exception {
		MqttConnectOptions options = TestClients.options(relayURI, name.getMethodName());
		options.setWill(willTopic, WILL, 1, false);
		MqttClient client = new MqttClient(options);
		client.connect();
		return client;
	}

	/**
	 * A client connected straight to the broker, subscribed to a will topic.
	 */
	private final class WillWatcher implements AutoCloseable {
		final String topic;
		final MqttV3Receiver receiver = new MqttV3Receiver();
		private final MqttAsyncClient client;

		/**
		 * Connects and subscribes.
		 *
		 * @param topic the will topic
		 * @throws Exception if the connection fails
		 */
		WillWatcher(String topic) throws Exception {
			this.topic = topic;
			client = TestClients.createMqttAsyncClient(broker.getServerURI(), name.getMethodName() + "-watcher");
			client.setCallback(receiver);
			client.connect().waitForCompletion(5000);
			client.subscribe(topic, 1).waitForCompletion(5000);
		}

		/**
		 * Disconnects the watcher.
		 */
		@Override
		public void close() {
			TestClients.shutdownQuietly(client);
		}
	}
}
