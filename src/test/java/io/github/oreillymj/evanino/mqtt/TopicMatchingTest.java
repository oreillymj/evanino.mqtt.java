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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Matching incoming topic names against subscription filters, which decides the listener a
 * message is delivered to. The rules are MQTT 3.1.1 section 4.7. Not part of Paho's suite. The
 * delivery tests require a broker; see {@link MosquittoBroker}.
 */
public class TopicMatchingTest {

	/** The broker used by the delivery tests. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID. */
	@Rule
	public final TestName name = new TestName();

	private static URI serverURI;
	private static String topicPrefix;

	/**
	 * Picks up the broker URI and a unique topic prefix for the run.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverURI = broker.getServerURI();
		topicPrefix = "TopicMatchingTest-" + UUID.randomUUID().toString().replace("-", "");
	}

	/**
	 * Filters and topic names that must match.
	 */
	@Test
	public void testMatches() {
		String[][] cases = {
				// exact
				{"sport/tennis", "sport/tennis"},
				{"/", "/"},
				// + matches exactly one level, which may be empty
				{"sport/+", "sport/tennis"},
				{"sport/+/player1", "sport/tennis/player1"},
				{"+/+", "/finance"},
				{"/+", "/finance"},
				{"a/+/c", "a//c"},
				{"sport/+", "sport/"},
				{"+", "sport"},
				// # matches any number of levels, including none (the parent level)
				{"sport/#", "sport"},
				{"sport/#", "sport/"},
				{"sport/#", "sport/tennis"},
				{"sport/#", "sport/tennis/player1"},
				{"sport/tennis/#", "sport/tennis"},
				{"#", "sport/tennis/player1"},
				{"#", "/"},
				{"+/tennis/#", "sport/tennis/player1/ranking"},
				// $ topics need a filter starting with $
				{"$SYS/#", "$SYS/broker/uptime"},
				{"$SYS/+/uptime", "$SYS/broker/uptime"},
				// characters that are special in regular expressions are literal
				{"a.b", "a.b"},
				{"a$b/c", "a$b/c"},
				{"price(usd)/[0]", "price(usd)/[0]"},
				{"a*b+", "a*b+"},
		};
		for (String[] c : cases) {
			Assert.assertTrue("'" + c[0] + "' should match '" + c[1] + "'", MqttSubscription.matches(c[0], c[1]));
		}
	}

	/**
	 * Filters and topic names that must not match.
	 */
	@Test
	public void testDoesNotMatch() {
		String[][] cases = {
				{"sport/tennis", "sport/tennis/player1"},
				{"sport/tennis", "sport"},
				{"sport/tennis", "Sport/tennis"},
				// + is one level only
				{"sport/+", "sport"},
				{"sport/+", "sport/tennis/player1"},
				{"+", "/finance"},
				{"+", "sport/tennis"},
				// # does not match a different parent
				{"sport/#", "sports"},
				{"sport/tennis/#", "sport/tennisball"},
				// a leading wildcard does not match a $ topic (section 4.7.2)
				{"#", "$SYS/broker/uptime"},
				{"+/broker/uptime", "$SYS/broker/uptime"},
				{"+/#", "$SYS/broker"},
				// regular-expression characters are literal, not patterns
				{"a.b", "aXb"},
				{"a*b", "aaab"},
				{"a+", "aa"},
				{"(x|y)", "x"},
				{"[ab]", "a"},
		};
		for (String[] c : cases) {
			Assert.assertFalse("'" + c[0] + "' should not match '" + c[1] + "'", MqttSubscription.matches(c[0], c[1]));
		}
	}

	/**
	 * A listener on {@code parent/#} receives a message published to {@code parent} itself:
	 * the broker delivers it, and the client must route it to that listener rather than to the
	 * default callback.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testHashListenerReceivesParentLevel() throws Exception {
		String parent = topicPrefix + "/" + name.getMethodName();
		MqttV3Receiver defaultCallback = new MqttV3Receiver();
		MqttAsyncClient client = connect(defaultCallback);
		try {
			PerSubscriptionMessageHandlerTest.Listener listener = new PerSubscriptionMessageHandlerTest.Listener();
			client.subscribe(parent + "/#", 1, listener).waitForCompletion(5000);

			TestClients.publish(client, parent, "parent".getBytes(StandardCharsets.UTF_8), 1, false).waitForCompletion(5000);

			MqttMessage received = listener.getNextMessage();
			Assert.assertNotNull("listener on parent/# did not receive a message on parent", received);
			Assert.assertEquals("parent", received.getPayloadString());
			Assert.assertNull("message went to the default callback instead", defaultCallback.receiveNext(1000));
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * A {@code .} in a filter is literal: with listeners on {@code a.b} and {@code aXb}, a
	 * message on {@code aXb} reaches only the second.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testDotInFilterIsLiteral() throws Exception {
		String base = topicPrefix + "/" + name.getMethodName() + "/";
		MqttAsyncClient client = connect(new MqttV3Receiver());
		try {
			PerSubscriptionMessageHandlerTest.Listener dotted = new PerSubscriptionMessageHandlerTest.Listener();
			PerSubscriptionMessageHandlerTest.Listener plain = new PerSubscriptionMessageHandlerTest.Listener();
			client.subscribe(base + "a.b", 1, dotted).waitForCompletion(5000);
			client.subscribe(base + "aXb", 1, plain).waitForCompletion(5000);

			TestClients.publish(client, base + "aXb", "x".getBytes(StandardCharsets.UTF_8), 1, false).waitForCompletion(5000);

			Assert.assertNotNull("listener on aXb did not receive the message", plain.getNextMessage());
			Thread.sleep(1000);
			Assert.assertNull("listener on a.b received a message on aXb", dotted.getNextMessageNow());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * A listener on {@code a/+/c} receives a message on {@code a//c}, where the {@code +}
	 * level is empty.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testPlusMatchesEmptyLevel() throws Exception {
		String base = topicPrefix + "/" + name.getMethodName();
		MqttV3Receiver defaultCallback = new MqttV3Receiver();
		MqttAsyncClient client = connect(defaultCallback);
		try {
			PerSubscriptionMessageHandlerTest.Listener listener = new PerSubscriptionMessageHandlerTest.Listener();
			client.subscribe(base + "/+/c", 1, listener).waitForCompletion(5000);

			TestClients.publish(client, base + "//c", "empty".getBytes(StandardCharsets.UTF_8), 1, false).waitForCompletion(5000);

			MqttMessage received = listener.getNextMessage();
			Assert.assertNotNull("listener on a/+/c did not receive a message on a//c", received);
			Assert.assertNull("message went to the default callback instead", defaultCallback.receiveNext(1000));
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Connects an asynchronous client with the given default callback.
	 *
	 * @param callback the default callback
	 * @return the connected client
	 * @throws Exception if the connection fails
	 */
	private MqttAsyncClient connect(MqttCallback callback) throws Exception {
		MqttAsyncClient client = TestClients.createMqttAsyncClient(serverURI, name.getMethodName());
		client.setCallback(callback);
		client.connect().waitForCompletion(5000);
		return client;
	}
}
