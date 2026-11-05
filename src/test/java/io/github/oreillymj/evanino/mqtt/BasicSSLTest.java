/*******************************************************************************
 * Ported from Eclipse Paho's Java client tests,
 * org/eclipse/paho/client/mqttv3/test/BasicSSLTest.java,
 * https://github.com/eclipse-paho/paho.mqtt.java. This project uses the Paho code under
 * the Eclipse Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 * The original notice:
 *
 *   Copyright (c) 2009, 2014 IBM Corp.
 *
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Eclipse Public License v2.0
 *   and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 *   The Eclipse Public License is available at
 *      https://www.eclipse.org/legal/epl-2.0
 *   and the Eclipse Distribution License is available at
 *     https://www.eclipse.org/org/documents/edl-v10.php
 *
 * Changes copyright (c) 2026 Michael OReilly, under the Apache License 2.0 (see LICENSE).
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

/**
 * Port of Paho's {@code BasicSSLTest}: publish/subscribe over TLS with a client certificate,
 * and refusal of mismatched TLS / plain connections. Requires a broker with a TLS listener;
 * see {@link MosquittoBroker}.
 * <p>
 * Differences from Paho:
 * <ul>
 * <li>The broker URI uses {@code mqtts://}, an alias of Paho's {@code ssl://};
 * {@link #testSslSchemeAlias()} checks that {@code ssl://} works too.</li>
 * <li>Paho configures TLS through the {@code javax.net.ssl.*} system properties. This client
 * only uses TLS when an {@code SSLContext} is set in the options, so the tests set one built
 * from the same kind of keystore and truststore ({@link TestTls#clientContext()}).</li>
 * </ul>
 */
public class BasicSSLTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID. */
	@Rule
	public final TestName name = new TestName();

	private static final int messageSize = 100000;
	private static URI sslServerURI;
	private static String topicPrefix;

	/**
	 * Picks up the TLS broker URI and a unique topic prefix for the run.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		sslServerURI = broker.getSslServerURI();
		topicPrefix = "BasicSSLTest-" + UUID.randomUUID() + "-";
	}

	/**
	 * Creates a TLS client that trusts the test CA and presents the test client certificate.
	 *
	 * @param serverURI the broker URI
	 * @param clientId  the client identifier
	 * @return a new, unconnected client
	 * @throws Exception if the TLS context cannot be created
	 */
	static MqttClient createSslClient(URI serverURI, String clientId) throws Exception {
		MqttConnectOptions options = TestClients.options(serverURI, clientId);
		options.setSslContext(TestTls.clientContext());
		return new MqttClient(options);
	}

	/**
	 * A QoS 2 message makes the round trip over TLS.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testSSL() throws Exception {
		String methodName = name.getMethodName();
		MqttClient mqttClient = null;
		try {
			mqttClient = createSslClient(sslServerURI, methodName);
			MqttV3Receiver mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			mqttClient.connect(mqttClient.getOptions());

			String[] topicNames = new String[]{topicPrefix + methodName + "/Topic"};
			int[] topicQos = {2};
			mqttClient.subscribeWithResponse(topicNames, topicQos, null);

			byte[] payload = ("Message payload " + getClass().getName() + "." + methodName).getBytes(StandardCharsets.UTF_8);
			MqttTopic mqttTopic = mqttClient.getTopic(topicNames[0]);
			mqttTopic.publish(payload, 2, false);
			boolean ok = mqttV3Receiver.validateReceipt(topicNames[0], 2, payload);
			if (!ok) {
				Assert.fail("Receive failed");
			}
		} catch (Exception exception) {
			Assert.fail("Failed to instantiate:" + methodName + " exception=" + exception);
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * {@code ssl://} is an alias of {@code mqtts://}: a QoS 1 message makes the round trip over
	 * TLS straight to the broker, with no proxy.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testSslSchemeAlias() throws Exception {
		URI sslURI = new URI("ssl", null, sslServerURI.getHost(), sslServerURI.getPort(), null, null, null);
		MqttClient mqttClient = createSslClient(sslURI, name.getMethodName());
		try {
			MqttV3Receiver mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			mqttClient.connect(mqttClient.getOptions());
			Assert.assertNull("ssl:// must not configure a proxy", mqttClient.getOptions().getProxy());

			String topic = topicPrefix + name.getMethodName() + "/Topic";
			mqttClient.subscribe(topic, 1);
			byte[] payload = "over ssl://".getBytes(StandardCharsets.UTF_8);
			mqttClient.publish(topic, payload, 1, false);
			Assert.assertTrue("Receive failed", mqttV3Receiver.validateReceipt(topic, 1, payload));
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * Every message from four TLS publishers reaches each of twenty TLS subscribers.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testSSLWorkload() throws Exception {
		MqttClient[] mqttPublisher = new MqttClient[4];
		MqttClient[] mqttSubscriber = new MqttClient[20];
		try {
			String[] topicNames = new String[]{topicPrefix + name.getMethodName() + "/Topic"};
			int[] topicQos = {0};

			MqttTopic[] mqttTopic = new MqttTopic[mqttPublisher.length];
			for (int i = 0; i < mqttPublisher.length; i++) {
				mqttPublisher[i] = createSslClient(sslServerURI, "MultiPub" + i);
				mqttPublisher[i].connect(mqttPublisher[i].getOptions());
				mqttTopic[i] = mqttPublisher[i].getTopic(topicNames[0]);
			}

			MqttV3Receiver[] mqttV3Receiver = new MqttV3Receiver[mqttSubscriber.length];
			for (int i = 0; i < mqttSubscriber.length; i++) {
				mqttSubscriber[i] = createSslClient(sslServerURI, "MultiSubscriber" + i);
				mqttV3Receiver[i] = new MqttV3Receiver();
				mqttSubscriber[i].setCallback(mqttV3Receiver[i]);
				mqttSubscriber[i].connect(mqttSubscriber[i].getOptions());
				mqttSubscriber[i].subscribeWithResponse(topicNames, topicQos, null);
			}

			for (int iMessage = 0; iMessage < 10; iMessage++) {
				byte[] payload = ("Message " + iMessage).getBytes(StandardCharsets.UTF_8);
				for (MqttTopic topic : mqttTopic) {
					topic.publish(payload, 0, false);
				}

				for (MqttV3Receiver receiver : mqttV3Receiver) {
					for (int ii = 0; ii < mqttPublisher.length; ii++) {
						boolean ok = receiver.validateReceipt(topicNames[0], 0, payload);
						if (!ok) {
							Assert.fail("Receive failed");
						}
					}
				}
			}
		} finally {
			for (MqttClient publisher : mqttPublisher) {
				TestClients.closeQuietly(publisher);
			}
			for (MqttClient subscriber : mqttSubscriber) {
				TestClients.closeQuietly(subscriber);
			}
		}
	}

	/**
	 * A 100,000-byte QoS 2 message makes the round trip over TLS. The decoder accepts 64 KB
	 * packets by default, so the limit is raised for this test.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testSSLLargeMessage() throws Exception {
		String methodName = name.getMethodName();
		MqttClient mqttClient = null;
		try {
			MqttConnectOptions options = TestClients.options(sslServerURI, methodName);
			options.setSslContext(TestTls.clientContext());
			options.setMaxBytesInMessage(messageSize + 1024);
			mqttClient = new MqttClient(options);
			MqttV3Receiver mqttV3Receiver = new MqttV3Receiver();
			mqttClient.setCallback(mqttV3Receiver);
			mqttClient.connect(options);

			String[] topicNames = new String[]{topicPrefix + methodName + "/Topic"};
			int[] topicQos = {2};
			mqttClient.subscribeWithResponse(topicNames, topicQos, null);

			// Create message of size 'messageSize'
			byte[] message = new byte[messageSize];
			Arrays.fill(message, (byte) 's');
			MqttTopic mqttTopic = mqttClient.getTopic(topicNames[0]);
			mqttTopic.publish(message, 2, false);
			boolean ok = mqttV3Receiver.validateReceipt(topicNames[0], 2, message);
			if (!ok) {
				Assert.fail("Receive failed");
			}
		} catch (Exception exception) {
			Assert.fail("Failed:" + methodName + " exception=" + exception);
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * A plain TCP connection to the TLS listener is refused.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testNonSSLtoSSLChannel() throws Exception {
		MqttClient mqttClient = null;
		try {
			URI plainToSsl = new URI("tcp://" + sslServerURI.getHost() + ":" + sslServerURI.getPort());
			mqttClient = TestClients.createMqttClient(plainToSsl, name.getMethodName());
			mqttClient.setTimeToWait(5000);
			mqttClient.setCallback(new MqttV3Receiver());
			try {
				mqttClient.connect(mqttClient.getOptions());
				Assert.fail("Non SSL Connection was allowed to SSL channel with Client Authentication");
			} catch (MqttException e) {
				// Expected exception
			}
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * A TLS connection to the plain TCP listener is refused.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSSLtoNonSSLChannel() throws Exception {
		MqttClient mqttClient = null;
		try {
			URI sslToPlain = new URI("mqtts://localhost:" + broker.getServerURI().getPort());
			mqttClient = createSslClient(sslToPlain, name.getMethodName());
			mqttClient.setTimeToWait(5000);
			mqttClient.setCallback(new MqttV3Receiver());
			try {
				mqttClient.connect(mqttClient.getOptions());
				Assert.fail("SSL Connection was allowed to a channel without SSL");
			} catch (MqttException e) {
				// Expected exception
			}
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}
}
