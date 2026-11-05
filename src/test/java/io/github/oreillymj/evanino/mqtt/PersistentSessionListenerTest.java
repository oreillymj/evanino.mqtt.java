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
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Subscriptions and their per-subscription listeners across a reconnect. With
 * {@code cleanSession=false} the broker keeps the subscriptions and sends their messages after
 * the reconnect, so the listeners are kept too, as Paho keeps them; they used to be cleared on
 * connection loss, so those messages were acknowledged and lost. With a clean session, or when
 * CONNACK reports the session gone, the listeners are dropped, so subscribing again does not
 * deliver twice. Not part of Paho's suite (found while testing Paho issue 1098); uses
 * {@link MosquittoBroker} and {@link RawBroker}.
 */
public class PersistentSessionListenerTest {

	/** The broker. */
	@ClassRule
	public static final MosquittoBroker BROKER = new MosquittoBroker();

	/**
	 * Subscribes with a listener, drops the connection, queues a message while disconnected,
	 * and expects the listener to receive it after the automatic reconnect.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testListenerReceivesMessageAfterReconnect() throws Exception {
		ConnectionManipulationProxyServer proxy = new ConnectionManipulationProxyServer(
				"127.0.0.1", BROKER.getServerURI().getPort(), 0);
		proxy.startProxy();
		java.net.URI proxyUri = java.net.URI.create("tcp://127.0.0.1:" + proxy.getLocalPort());
		MqttAsyncClient client = TestClients.createMqttAsyncClient(proxyUri, "PersistentSessionListener");
		MqttAsyncClient direct = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PersistentSessionListener-pub");
		List<String> received = new CopyOnWriteArrayList<>();
		try {
			MqttConnectOptions o = TestClients.options(proxyUri, "PersistentSessionListener");
			o.setCleanSession(false);
			o.setAutomaticReconnect(true);
			o.setRetryInterval(1);
			o.setMaxReconnectDelay(2000);
			client.connect(o).waitForCompletion(10_000);
			client.subscribe("PersistentSessionListener/t", 1,
					(t, m) -> received.add(new String(m.getPayload(), StandardCharsets.UTF_8))).waitForCompletion(10_000);
			direct.connect(TestClients.options(BROKER.getServerURI(), "PersistentSessionListener-pub")).waitForCompletion(10_000);

			direct.publish("PersistentSessionListener/t", "before".getBytes(StandardCharsets.UTF_8), 1, false).waitForCompletion(10_000);
			waitFor(() -> received.contains("before"), 10_000);

			proxy.disableProxy();
			waitFor(() -> !client.isConnected(), 10_000);
			direct.publish("PersistentSessionListener/t", "while-down".getBytes(StandardCharsets.UTF_8), 1, false).waitForCompletion(10_000);
			proxy.enableProxy();
			waitFor(client::isConnected, 20_000);
			direct.publish("PersistentSessionListener/t", "after".getBytes(StandardCharsets.UTF_8), 1, false).waitForCompletion(10_000);
			Thread.sleep(2000);

			System.out.println("listener received " + received);
			Assert.assertTrue("queued message lost; listener received " + received, received.contains("while-down"));
			Assert.assertTrue("message after reconnect lost; listener received " + received, received.contains("after"));
		} finally {
			TestClients.shutdownQuietly(client);
			TestClients.shutdownQuietly(direct);
			proxy.stopProxy();
		}
	}

	/**
	 * With a clean session, an application that subscribes again after a reconnect, with a new
	 * listener, gets each message once, on the new listener only.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testCleanSessionResubscribeDeliversOnce() throws Exception {
		ConnectionManipulationProxyServer proxy = new ConnectionManipulationProxyServer(
				"127.0.0.1", BROKER.getServerURI().getPort(), 0);
		proxy.startProxy();
		java.net.URI proxyUri = java.net.URI.create("tcp://127.0.0.1:" + proxy.getLocalPort());
		MqttAsyncClient client = TestClients.createMqttAsyncClient(proxyUri, "CleanSessionListener");
		MqttAsyncClient direct = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "CleanSessionListener-pub");
		List<String> first = new CopyOnWriteArrayList<>();
		List<String> second = new CopyOnWriteArrayList<>();
		try {
			MqttConnectOptions o = TestClients.options(proxyUri, "CleanSessionListener");
			o.setAutomaticReconnect(true);
			o.setRetryInterval(1);
			o.setMaxReconnectDelay(2000);
			client.connect(o).waitForCompletion(10_000);
			client.subscribe("CleanSessionListener/t", 1, (t, m) -> first.add("m")).waitForCompletion(10_000);
			proxy.disableProxy();
			waitFor(() -> !client.isConnected(), 10_000);
			proxy.enableProxy();
			waitFor(client::isConnected, 20_000);
			client.subscribe("CleanSessionListener/t", 1, (t, m) -> second.add("m")).waitForCompletion(10_000);
			direct.connect(TestClients.options(BROKER.getServerURI(), "CleanSessionListener-pub")).waitForCompletion(10_000);
			direct.publish("CleanSessionListener/t", "x".getBytes(StandardCharsets.UTF_8), 1, false).waitForCompletion(10_000);
			Thread.sleep(1500);
			Assert.assertEquals("old listener", 0, first.size());
			Assert.assertEquals("new listener", 1, second.size());
		} finally {
			TestClients.shutdownQuietly(client);
			TestClients.shutdownQuietly(direct);
			proxy.stopProxy();
		}
	}

	/**
	 * With cleanSession=false, a CONNACK without session present (the broker lost the session)
	 * drops the old listeners: after subscribing again, a message reaches the new listener only.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testLostSessionDropsListeners() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = new MqttAsyncClient(broker.uri().toString(), "LostSessionListener");
			List<String> first = new CopyOnWriteArrayList<>();
			List<String> second = new CopyOnWriteArrayList<>();
			try {
				MqttConnectOptions o = TestClients.options(broker.uri(), "LostSessionListener");
				o.setCleanSession(false);
				IMqttToken c = client.connect(o);
				broker.acceptAndConnack(0);
				c.waitForCompletion(5000);
				IMqttToken s1 = client.subscribe("t", 1, (t, m) -> first.add("m"));
				broker.suback(broker.readUntil(8));
				s1.waitForCompletion(5000);

				broker.drop();
				waitFor(() -> !client.isConnected(), 5000);
				IMqttToken c2 = client.connect(o);
				broker.acceptAndConnack(0);            // session present = 0
				c2.waitForCompletion(5000);
				IMqttToken s2 = client.subscribe("t", 1, (t, m) -> second.add("m"));
				broker.suback(broker.readUntil(8));
				s2.waitForCompletion(5000);
				broker.publish("t", 0, 0, "x");
				Thread.sleep(1000);
				Assert.assertEquals("old listener", 0, first.size());
				Assert.assertEquals("new listener", 1, second.size());
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	private static void waitFor(java.util.function.BooleanSupplier condition, long millis) throws InterruptedException {
		long deadline = System.currentTimeMillis() + millis;
		while (!condition.getAsBoolean()) {
			if (System.currentTimeMillis() > deadline) {
				Assert.fail("timed out");
			}
			Thread.sleep(50);
		}
	}
}
