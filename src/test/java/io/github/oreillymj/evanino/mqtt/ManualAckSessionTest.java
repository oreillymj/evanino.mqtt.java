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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Incoming QoS 2 messages with manual acknowledgements across a reconnect with
 * {@code cleanSession=false}: a message acknowledged before the connection dropped is not
 * delivered again when the broker sends its PUBREL again, and one not yet acknowledged is
 * delivered again and can then be acknowledged. The subscriber connects through a relay that
 * holds the broker's replies back for {@value #REPLY_DELAY_MILLIS} ms, so the connection can be
 * dropped between the steps. Not part of Paho's suite. Requires a broker; see
 * {@link MosquittoBroker}.
 */
public class ManualAckSessionTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID and topic. */
	@Rule
	public final TestName name = new TestName();

	/** How long the relay holds back each reply from the broker. */
	private static final long REPLY_DELAY_MILLIS = 800;

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
	 * A message acknowledged before the connection dropped, whose PUBREL had not yet arrived, is
	 * not delivered again after the reconnect.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testAcknowledgedMessageIsNotDeliveredAgain() throws Exception {
		run(true);
	}

	/**
	 * A message not yet acknowledged when the connection dropped is delivered again after the
	 * reconnect, and once acknowledged is not delivered a third time.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testUnacknowledgedMessageIsDeliveredAgain() throws Exception {
		run(false);
	}

	/**
	 * Delivers one QoS 2 message to a manual-ack subscriber, acknowledges it before dropping the
	 * connection or not, and counts the deliveries after the reconnect.
	 *
	 * @param ackBeforeDrop whether to acknowledge the first delivery before the drop
	 * @throws Exception if the test fails unexpectedly
	 */
	private void run(boolean ackBeforeDrop) throws Exception {
		String topic = "ManualAckSessionTest/" + name.getMethodName() + "/" + UUID.randomUUID();
		MqttConnectOptions options = TestClients.options(relayURI, name.getMethodName());
		options.setCleanSession(false);
		options.setManualAcks(true);
		options.setAutomaticReconnect(true);
		options.setMaxRetries(-1);
		options.setConnectionTimeout(10);
		MqttAsyncClient subscriber = new MqttAsyncClient(options);
		BlockingQueue<MqttMessage> deliveries = new LinkedBlockingQueue<>();
		subscriber.setCallback(new MqttCallbackAdapter() {
			@Override
			public void messageArrived(String t, MqttMessage message) {
				deliveries.add(message);
			}
		});
		MqttAsyncClient publisher = TestClients.createMqttAsyncClient(broker.getServerURI(), name.getMethodName() + "-pub");
		try {
			subscriber.connect().waitForCompletion(10_000);
			subscriber.subscribe(topic, 2).waitForCompletion(10_000);
			publisher.connect().waitForCompletion(5000);
			publisher.publish(topic, new byte[]{7}, 2, false).waitForCompletion(5000);

			MqttMessage first = deliveries.poll(10, TimeUnit.SECONDS);
			Assert.assertNotNull("the message was not delivered", first);
			if (ackBeforeDrop) {
				subscriber.messageArrivedComplete(first.getId(), 2);   // sends PUBREC
				Thread.sleep(200);                                      // PUBREL is held back by the relay
			}
			relay.dropConnections();

			if (ackBeforeDrop) {
				Assert.assertNull("an acknowledged message was delivered again", deliveries.poll(6, TimeUnit.SECONDS));
			} else {
				MqttMessage again = deliveries.poll(10, TimeUnit.SECONDS);
				Assert.assertNotNull("an unacknowledged message should be delivered again", again);
				Assert.assertArrayEquals(new byte[]{7}, again.getPayload());
				subscriber.messageArrivedComplete(again.getId(), 2);
				Assert.assertNull("delivered a third time", deliveries.poll(4, TimeUnit.SECONDS));
			}
			Assert.assertTrue("the subscriber should be connected again", subscriber.isConnected());

			// The session still works: a new message arrives once.
			publisher.publish(topic, new byte[]{8}, 2, false).waitForCompletion(5000);
			MqttMessage next = deliveries.poll(10, TimeUnit.SECONDS);
			Assert.assertNotNull("a later message was not delivered", next);
			Assert.assertArrayEquals(new byte[]{8}, next.getPayload());
			subscriber.messageArrivedComplete(next.getId(), 2);
			Assert.assertNull(deliveries.poll(3, TimeUnit.SECONDS));
		} finally {
			TestClients.shutdownQuietly(publisher);
			TestClients.shutdownQuietly(subscriber);
		}
	}
}
