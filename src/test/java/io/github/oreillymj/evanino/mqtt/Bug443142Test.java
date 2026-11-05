/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/Bug443142Test.java,
 * https://github.com/eclipse-paho/paho.mqtt.java. This project uses the Paho code under
 * the Eclipse Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 * The original file has no notice of its own; it is part of Eclipse Paho, copyright
 * IBM Corp. and the other Paho contributors, under the Eclipse Public License v2.0
 * and the Eclipse Distribution License v1.0.
 *
 * Changes copyright (c) 2026 Michael OReilly, under the Apache License 2.0 (see LICENSE).
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Port of Paho's {@code Bug443142Test}: a {@code messageArrived} callback that blocks and then
 * throws must not deadlock the client, and, per Paho's contract, the throwing client is shut
 * down while other clients stay connected. Requires a broker; see {@link MosquittoBroker}.
 */
public class Bug443142Test {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	private static URI serverURI;
	private static String topicPrefix;

	/**
	 * Picks up the broker URI and a unique topic prefix for the run.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverURI = broker.getServerURI();
		topicPrefix = "Bug443142Test-" + UUID.randomUUID() + "-";
	}

	/**
	 * Client 2's callback sleeps then throws on the first of sixteen messages from client 1;
	 * afterwards client 1 must still be connected and client 2 disconnected.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testBug443142() throws Exception {
		CountDownLatch stopLatch = new CountDownLatch(1);
		MqttClient client1 = TestClients.createMqttClient(serverURI, "Bug443142Test-" + UUID.randomUUID());
		MqttClient client2 = TestClients.createMqttClient(serverURI, "Bug443142Test-" + UUID.randomUUID());
		try {
			client1.connect(client1.getOptions());

			// Paho disconnects a client whose messageArrived throws; here that is opt-in.
			client2.getOptions().setMessageArrivedErrorsTriggerDisconnect(true);
			client2.setCallback(new MyMqttCallback(stopLatch));
			client2.connect(client2.getOptions());
			String barTopic = topicPrefix + "bar";
			client2.subscribe(barTopic, 1);

			// publish messages until the queue is full > 10
			for (int i = 0; i < 16; i++) {
				MqttMessage message = new MqttMessage(("foo-" + i).getBytes(StandardCharsets.UTF_8));
				client1.publish(barTopic, message);
			}

			// wait until the exception is thrown
			stopLatch.await();
			// wait some time let client2 to shutdown because of the exception thrown from the callback
			Thread.sleep(5000);

			// client2 should be disconnected
			Assert.assertTrue("client1 should connected", client1.isConnected());
			Assert.assertFalse("client2 should disconnected", client2.isConnected());

			// close client1
			client1.disconnect();
			client1.close();
			Assert.assertFalse("client1 should disconnected", client1.isConnected());
		} finally {
			TestClients.closeQuietly(client1);
			TestClients.closeQuietly(client2);
		}
	}

	/**
	 * Callback that blocks for five seconds on the first message, then throws.
	 */
	private static class MyMqttCallback extends MqttCallbackAdapter {
		private final CountDownLatch stopLatch;

		/**
		 * Creates the callback.
		 *
		 * @param stopLatch counted down just before the exception is thrown
		 */
		MyMqttCallback(CountDownLatch stopLatch) {
			this.stopLatch = stopLatch;
		}

		/**
		 * Sleeps, signals the test, then throws.
		 *
		 * @param topic   the topic the message was published to
		 * @param message the received message
		 */
		@Override
		public void messageArrived(String topic, MqttMessage message) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			stopLatch.countDown();
			throw new RuntimeException("deadlock");
		}
	}
}
