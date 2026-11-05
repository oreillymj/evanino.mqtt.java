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
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

/**
 * Incoming publishes at each QoS level, with automatic and with manual acknowledgements. Not
 * part of Paho's suite. Requires a broker; see {@link MosquittoBroker}.
 * <p>
 * Whether an acknowledgement really reached the broker is checked through the broker's session
 * state: the subscriber uses {@code cleanSession=false}, so after a reconnect the broker
 * redelivers a QoS 1 or 2 message it has not had acknowledged, and does not redeliver one it has.
 */
@RunWith(Parameterized.class)
public class PublishAcknowledgementTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** How long to wait for a message that is expected not to arrive. */
	private static final long NO_MESSAGE_WAIT_MILLIS = 2000;

	private static URI serverURI;
	private static String topicPrefix;

	private final int qos;
	private final boolean manualAcks;

	/**
	 * Returns every combination of QoS level and acknowledgement mode.
	 *
	 * @return the test parameters: QoS level, manual acknowledgements
	 */
	@Parameterized.Parameters(name = "qos={0}, manualAcks={1}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
				{0, false}, {1, false}, {2, false},
				{0, true}, {1, true}, {2, true},
		});
	}

	/**
	 * Creates the test for one combination.
	 *
	 * @param qos        the QoS the message is published and received at
	 * @param manualAcks whether the subscriber acknowledges messages manually
	 */
	public PublishAcknowledgementTest(int qos, boolean manualAcks) {
		this.qos = qos;
		this.manualAcks = manualAcks;
	}

	/**
	 * Picks up the broker URI and a unique topic prefix for the run.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverURI = broker.getServerURI();
		topicPrefix = "PublishAcknowledgementTest-" + UUID.randomUUID() + "-";
	}

	/**
	 * A message published at the test's QoS arrives at that QoS with its payload, and the
	 * publisher's delivery token completes. With manual acknowledgements the message is then
	 * acknowledged with {@code messageArrivedComplete}.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testPublishIsReceived() throws Exception {
		String id = clientId("received");
		String topic = topicPrefix + id;
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttClient subscriber = connectSubscriber(id, receiver);
		MqttClient publisher = connectPublisher(id);
		try {
			subscriber.subscribe(topic, 2);

			byte[] payload = ("message at qos " + qos).getBytes(StandardCharsets.UTF_8);
			publish(publisher, topic, payload);

			MqttV3Receiver.ReceivedMessage received = receiver.receiveNext(MqttV3Receiver.VALIDATE_WAIT_MILLIS);
			Assert.assertNotNull("message not received", received);
			Assert.assertEquals(topic, received.topic);
			Assert.assertArrayEquals(payload, received.message.getPayload());
			Assert.assertEquals(qos, received.message.getQos());

			if (manualAcks) {
				subscriber.messageArrivedComplete(received.message.getId(), received.message.getQos());
			}
		} finally {
			TestClients.closeQuietly(publisher);
			TestClients.closeQuietly(subscriber);
		}
	}

	/**
	 * Once a QoS 1 or 2 message has been acknowledged, automatically or with
	 * {@code messageArrivedComplete}, the broker does not redeliver it when the subscriber
	 * resumes its session.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testAcknowledgedMessageIsNotRedelivered() throws Exception {
		Assume.assumeTrue("QoS 0 messages are never acknowledged or redelivered", qos > 0);
		String id = clientId("acked");
		String topic = topicPrefix + id;
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttClient subscriber = connectSubscriber(id, receiver);
		MqttClient publisher = connectPublisher(id);
		try {
			subscriber.subscribe(topic, 2);
			byte[] payload = "acknowledge me".getBytes(StandardCharsets.UTF_8);
			publish(publisher, topic, payload);

			MqttV3Receiver.ReceivedMessage received = receiver.receiveNext(MqttV3Receiver.VALIDATE_WAIT_MILLIS);
			Assert.assertNotNull("message not received", received);
			if (manualAcks) {
				subscriber.messageArrivedComplete(received.message.getId(), received.message.getQos());
			}
			// let the acknowledgement (and, for QoS 2, the PUBREL / PUBCOMP exchange) complete
			Thread.sleep(500);

			subscriber.disconnect();
			subscriber.connect(subscriberOptions(id));

			Assert.assertNull("acknowledged message was redelivered", receiver.receiveNext(NO_MESSAGE_WAIT_MILLIS));
		} finally {
			TestClients.closeQuietly(publisher);
			cleanUpSession(subscriber, id);
		}
	}

	/**
	 * With manual acknowledgements, a QoS 1 or 2 message that is not acknowledged is redelivered
	 * when the subscriber resumes its session; acknowledging the redelivered message stops any
	 * further redelivery.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testUnacknowledgedMessageIsRedelivered() throws Exception {
		Assume.assumeTrue("only manual acknowledgements can leave a message unacknowledged", manualAcks);
		Assume.assumeTrue("QoS 0 messages are never acknowledged or redelivered", qos > 0);
		String id = clientId("unacked");
		String topic = topicPrefix + id;
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttClient subscriber = connectSubscriber(id, receiver);
		MqttClient publisher = connectPublisher(id);
		try {
			subscriber.subscribe(topic, 2);
			byte[] payload = "do not acknowledge me yet".getBytes(StandardCharsets.UTF_8);
			publish(publisher, topic, payload);

			MqttV3Receiver.ReceivedMessage first = receiver.receiveNext(MqttV3Receiver.VALIDATE_WAIT_MILLIS);
			Assert.assertNotNull("message not received", first);

			// Disconnect without acknowledging; the broker keeps the message in flight.
			subscriber.disconnect();
			subscriber.connect(subscriberOptions(id));

			MqttV3Receiver.ReceivedMessage redelivered = receiver.receiveNext(MqttV3Receiver.VALIDATE_WAIT_MILLIS);
			Assert.assertNotNull("unacknowledged message was not redelivered", redelivered);
			Assert.assertArrayEquals(payload, redelivered.message.getPayload());
			Assert.assertEquals(qos, redelivered.message.getQos());
			Assert.assertTrue("redelivered message must have a packet identifier to acknowledge",
					redelivered.message.getId() > 0);

			subscriber.messageArrivedComplete(redelivered.message.getId(), redelivered.message.getQos());
			Thread.sleep(500);

			subscriber.disconnect();
			subscriber.connect(subscriberOptions(id));
			Assert.assertNull("acknowledged message was redelivered again", receiver.receiveNext(NO_MESSAGE_WAIT_MILLIS));
		} finally {
			TestClients.closeQuietly(publisher);
			cleanUpSession(subscriber, id);
		}
	}

	/**
	 * With manual acknowledgements, QoS 1 and 2 messages are handed to the application one at a
	 * time: the next message arrives only after the current one has been acknowledged.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testManualAcksDeliverOneMessageAtATime() throws Exception {
		Assume.assumeTrue("only applies to manual acknowledgements", manualAcks);
		Assume.assumeTrue("QoS 0 messages are not acknowledged", qos > 0);
		String id = clientId("serial");
		String topic = topicPrefix + id;
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttClient subscriber = connectSubscriber(id, receiver);
		MqttClient publisher = connectPublisher(id);
		try {
			subscriber.subscribe(topic, 2);
			for (int i = 0; i < 3; i++) {
				publish(publisher, topic, ("message " + i).getBytes(StandardCharsets.UTF_8));
			}

			for (int i = 0; i < 3; i++) {
				MqttV3Receiver.ReceivedMessage received = receiver.receiveNext(MqttV3Receiver.VALIDATE_WAIT_MILLIS);
				Assert.assertNotNull("message " + i + " not received", received);
				Assert.assertEquals("message " + i, received.message.getPayloadString());
				Assert.assertNull("message " + (i + 1) + " arrived before message " + i + " was acknowledged",
						receiver.receiveNext(1000));
				subscriber.messageArrivedComplete(received.message.getId(), received.message.getQos());
			}
		} finally {
			TestClients.closeQuietly(publisher);
			cleanUpSession(subscriber, id);
		}
	}

	// -------------------------------------------------------------
	// Helper methods
	// -------------------------------------------------------------

	/**
	 * Returns a client identifier unique to this test and parameter combination.
	 *
	 * @param test a short name for the test
	 * @return the client identifier
	 */
	private String clientId(String test) {
		return "pubAck-" + test + "-q" + qos + (manualAcks ? "-manual" : "-auto");
	}

	/**
	 * Returns the subscriber's options: a persistent session, and manual acknowledgements if
	 * the test uses them.
	 *
	 * @param id the client identifier
	 * @return new connection options
	 */
	private MqttConnectOptions subscriberOptions(String id) {
		MqttConnectOptions options = TestClients.options(serverURI, id);
		options.setCleanSession(false);
		options.setManualAcks(manualAcks);
		return options;
	}

	/**
	 * Connects a subscriber that starts from an empty session and keeps it across reconnects.
	 *
	 * @param id       the client identifier
	 * @param receiver the callback that collects incoming messages
	 * @return the connected subscriber
	 * @throws Exception if the connection fails
	 */
	private MqttClient connectSubscriber(String id, MqttV3Receiver receiver) throws Exception {
		// Discard any session left by an earlier run, then connect with a persistent session.
		MqttConnectOptions clean = TestClients.options(serverURI, id);
		clean.setCleanSession(true);
		MqttClient client = new MqttClient(clean);
		client.connect(clean);
		client.disconnect();

		client.setCallback(receiver);
		client.connect(subscriberOptions(id));
		return client;
	}

	/**
	 * Connects a publisher.
	 *
	 * @param id the subscriber's client identifier, used to derive the publisher's
	 * @return the connected publisher
	 * @throws Exception if the connection fails
	 */
	private MqttClient connectPublisher(String id) throws Exception {
		MqttClient client = TestClients.createMqttClient(serverURI, id + "-pub");
		client.connect(client.getOptions());
		return client;
	}

	/**
	 * Publishes a payload at the test's QoS and waits for the delivery to complete.
	 *
	 * @param publisher the publishing client
	 * @param topic     the topic to publish to
	 * @param payload   the message payload
	 * @throws MqttException if the publish fails or times out
	 */
	private void publish(MqttClient publisher, String topic, byte[] payload) throws MqttException {
		publisher.publish(topic, payload, qos, false);
	}

	/**
	 * Closes the subscriber and removes its persistent session from the broker.
	 *
	 * @param subscriber the subscriber, or {@code null}
	 * @param id         the subscriber's client identifier
	 */
	private void cleanUpSession(MqttClient subscriber, String id) {
		TestClients.closeQuietly(subscriber);
		MqttConnectOptions clean = TestClients.options(serverURI, id);
		clean.setCleanSession(true);
		MqttClient client = new MqttClient(clean);
		try {
			client.connect(clean);
		} catch (Exception ignored) {
		}
		TestClients.closeQuietly(client);
	}
}
