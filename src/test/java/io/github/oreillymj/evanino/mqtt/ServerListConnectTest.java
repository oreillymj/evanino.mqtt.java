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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.ServerSocket;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Connecting to one of several brokers ({@link MqttServerList}) against a real broker: the
 * first connect skips a broker that is down, and after a lost connection automatic reconnect
 * moves to another broker. Broker A is a proxy in front of the broker, which the tests cut;
 * broker B is the broker itself. Requires a broker; see {@link MosquittoBroker}.
 */
public class ServerListConnectTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID. */
	@Rule
	public final TestName name = new TestName();

	private ConnectionManipulationProxyServer proxy;

	private String proxyURI;

	private String brokerURI;

	private final List<MqttAsyncClient> clients = new ArrayList<>();

	/**
	 * Starts a proxy in front of the broker, to act as broker A.
	 *
	 * @throws Exception if the proxy cannot be started
	 */
	@Before
	public void startProxy() throws Exception {
		URI serverURI = broker.getServerURI();
		brokerURI = serverURI.toString();
		proxy = new ConnectionManipulationProxyServer(serverURI.getHost(), serverURI.getPort(), 0);
		proxy.startProxy();
		while (!proxy.isPortSet()) {
			Thread.sleep(0);
		}
		proxyURI = "tcp://localhost:" + proxy.getLocalPort();
	}

	/**
	 * Closes the clients and stops the proxy.
	 */
	@After
	public void stop() {
		for (MqttAsyncClient client : clients) {
			TestClients.shutdownQuietly(client);
		}
		if (proxy != null) {
			proxy.stopProxy();
		}
	}

	/**
	 * Returns options with automatic reconnect on and a one-second retry interval.
	 *
	 * @param serverURI the broker
	 * @return the options
	 */
	private MqttConnectOptions options(String serverURI) {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setServerURI(serverURI);
		options.setClientId(name.getMethodName());
		options.setAutomaticReconnect(true);
		options.setRetryInterval(1);
		return options;
	}

	/**
	 * Creates a client that records the broker of each connection attempt and of each
	 * completed connection.
	 *
	 * @param options  the client's options
	 * @param attempts receives the broker of each attempt
	 * @param complete counted down on each completed connection after a reconnect
	 * @return the client
	 */
	private MqttAsyncClient client(MqttConnectOptions options, List<String> attempts, CountDownLatch complete) {
		MqttAsyncClient client = new MqttAsyncClient(options);
		clients.add(client);
		client.setCallback(new MqttCallbackAdapter() {
			@Override
			public void onConnecting(boolean connecting, String serverURI) {
				if (connecting) {
					attempts.add(serverURI);
				}
			}

			@Override
			public void onConnectComplete(boolean reconnect, String serverURI) {
				if (reconnect) {
					complete.countDown();
				}
			}
		});
		return client;
	}

	/**
	 * Returns a port nothing listens on.
	 *
	 * @return the port
	 * @throws Exception if no port can be found
	 */
	private static int deadPort() throws Exception {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
	}

	/**
	 * Returns the port of a broker address.
	 *
	 * @param serverURI the address
	 * @return its port
	 */
	private static int port(String serverURI) {
		return URI.create(serverURI).getPort();
	}

	/**
	 * The first connect skips a broker that is down and connects to the next, with both the
	 * asynchronous and the blocking client.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testConnectSkipsBrokerThatIsDown() throws Exception {
		String dead = "tcp://localhost:" + deadPort();
		List<String> attempts = Collections.synchronizedList(new ArrayList<>());
		MqttServerList servers = new MqttServerList(options(dead));
		servers.addServer(brokerURI);
		MqttAsyncClient client = client(options(dead), attempts, new CountDownLatch(1));
		client.connect(servers).waitForCompletion(10000);
		Assert.assertTrue(client.isConnected());
		Assert.assertEquals(port(brokerURI), port(client.getServerURI()));
		Assert.assertEquals(2, attempts.size());
		Assert.assertEquals(port(dead), port(attempts.get(0)));

		MqttClient blocking = new MqttClient(dead, name.getMethodName() + "-blocking");
		try {
			blocking.connect(servers);
			Assert.assertTrue(blocking.isConnected());
			Assert.assertEquals(port(brokerURI), port(blocking.getServerURI()));
			blocking.disconnect();
		} finally {
			blocking.close();
		}
	}

	/**
	 * In next-server mode, a lost connection to broker A moves the client to broker B at once.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testReconnectMovesToNextServer() throws Exception {
		List<String> attempts = Collections.synchronizedList(new ArrayList<>());
		CountDownLatch reconnected = new CountDownLatch(1);
		MqttServerList servers = new MqttServerList(options(proxyURI)).reconnectUsingNextServer(true);
		servers.addServer(brokerURI);
		MqttAsyncClient client = client(options(proxyURI), attempts, reconnected);
		client.connect(servers).waitForCompletion(10000);
		Assert.assertEquals(port(proxyURI), port(client.getServerURI()));

		proxy.disableProxy();   // broker A goes away
		Assert.assertTrue("no reconnect", reconnected.await(10, TimeUnit.SECONDS));
		Assert.assertTrue(client.isConnected());
		Assert.assertEquals(port(brokerURI), port(client.getServerURI()));
		Assert.assertEquals(2, attempts.size());   // A, then B
		Assert.assertEquals(port(brokerURI), port(attempts.get(1)));
	}

	/**
	 * In until-the-retry-limit mode, a lost connection to broker A is retried there until A's
	 * retry limit, and only then does the client move to broker B.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testReconnectUntilRetryLimit() throws Exception {
		List<String> attempts = Collections.synchronizedList(new ArrayList<>());
		CountDownLatch reconnected = new CountDownLatch(1);
		MqttConnectOptions a = options(proxyURI);
		a.setMaxRetries(2);   // the loss counts as the first failure, then two more attempts
		MqttServerList servers = new MqttServerList(a).reconnectUntilRetryLimit(true);
		servers.addServer(brokerURI);
		MqttAsyncClient client = client(options(proxyURI), attempts, reconnected);
		client.connect(servers).waitForCompletion(10000);

		proxy.disableProxy();
		Assert.assertTrue("no reconnect", reconnected.await(15, TimeUnit.SECONDS));
		Assert.assertEquals(port(brokerURI), port(client.getServerURI()));
		List<Integer> ports = new ArrayList<>();
		synchronized (attempts) {
			for (String uri : attempts) {
				ports.add(port(uri));
			}
		}
		int pa = port(proxyURI);
		int pb = port(brokerURI);
		Assert.assertEquals(java.util.Arrays.asList(pa, pa, pa, pb), ports);
	}

	/**
	 * Paho's {@code setServerURIs} on the options works through the client's own options and
	 * {@code connect()}.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testServerURIsWithConnect() throws Exception {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setClientId(name.getMethodName());
		options.setServerURIs(new String[]{"tcp://localhost:" + deadPort(), brokerURI});
		MqttAsyncClient client = client(options, Collections.synchronizedList(new ArrayList<>()), new CountDownLatch(1));
		client.connect().waitForCompletion(10000);
		Assert.assertTrue(client.isConnected());
		Assert.assertEquals(port(brokerURI), port(client.getServerURI()));
	}
}
