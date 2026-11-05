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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

/**
 * Connections through a SOCKS4, SOCKS5 or HTTP {@code CONNECT} proxy ({@link TestProxyServer})
 * over every transport: plain TCP, TLS, WebSockets and secure WebSockets, including the
 * {@code ssl://}, {@code http://} and {@code https://} aliases. Each round trip checks that a
 * message arrives and that the proxy opened a tunnel to the broker. Proxy authentication and
 * an unreachable proxy are covered too. Not part of Paho's suite. Requires a broker; see
 * {@link MosquittoBroker}.
 */
@RunWith(Parameterized.class)
public class ProxyTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID and topic. */
	@Rule
	public final TestName name = new TestName();

	private static String topicPrefix;

	private final TestProxyServer.Protocol protocol;
	private TestProxyServer proxy;

	/**
	 * Returns the proxy protocols to test.
	 *
	 * @return one parameter set per protocol
	 */
	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
				{TestProxyServer.Protocol.SOCKS4},
				{TestProxyServer.Protocol.SOCKS5},
				{TestProxyServer.Protocol.HTTP},
		});
	}

	/**
	 * Creates the test for one proxy protocol.
	 *
	 * @param protocol the proxy protocol
	 */
	public ProxyTest(TestProxyServer.Protocol protocol) {
		this.protocol = protocol;
	}

	/**
	 * Picks a unique topic prefix for the run.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		topicPrefix = "ProxyTest-" + UUID.randomUUID() + "-";
	}

	/**
	 * Starts a proxy that requires no authentication.
	 *
	 * @throws Exception if the proxy cannot be started
	 */
	@Before
	public void setUp() throws Exception {
		proxy = new TestProxyServer(protocol);
	}

	/**
	 * Stops the proxy.
	 */
	@After
	public void tearDown() {
		if (proxy != null) {
			proxy.close();
		}
	}

	/**
	 * Plain TCP through the proxy.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testTcp() throws Exception {
		roundTrip(broker.getServerURI(), false);
	}

	/**
	 * TLS ({@code mqtts://}) through the proxy.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testTls() throws Exception {
		roundTrip(broker.getSslServerURI(), true);
	}

	/**
	 * TLS with the {@code ssl://} alias through the proxy.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSslAlias() throws Exception {
		roundTrip(withScheme(broker.getSslServerURI(), "ssl"), true);
	}

	/**
	 * WebSockets ({@code ws://}) through the proxy.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testWebSocket() throws Exception {
		roundTrip(broker.getWebSocketServerURI(), false);
	}

	/**
	 * WebSockets with the {@code http://} alias through the proxy.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testHttpAlias() throws Exception {
		roundTrip(withScheme(broker.getWebSocketServerURI(), "http"), false);
	}

	/**
	 * Secure WebSockets ({@code wss://}) through the proxy.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSecureWebSocket() throws Exception {
		roundTrip(broker.getSecureWebSocketServerURI(), true);
	}

	/**
	 * Secure WebSockets with the {@code https://} alias through the proxy.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testHttpsAlias() throws Exception {
		roundTrip(withScheme(broker.getSecureWebSocketServerURI(), "https"), true);
	}

	/**
	 * A proxy that requires authentication accepts the right credentials: the user ID for
	 * SOCKS4, the username and password for SOCKS5 and HTTP.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testAuthenticatedProxy() throws Exception {
		proxy.close();
		proxy = new TestProxyServer(protocol, "proxyuser", "proxypass");
		roundTrip(broker.getServerURI(), false, "proxyuser", "proxypass");
	}

	/**
	 * A proxy that requires authentication refuses the wrong credentials (a wrong user ID for
	 * SOCKS4, a wrong password otherwise), and the connect fails rather than going to the
	 * broker directly.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testAuthenticatedProxyRejectsWrongCredentials() throws Exception {
		proxy.close();
		proxy = new TestProxyServer(protocol, "proxyuser", "proxypass");
		boolean socks4 = protocol == TestProxyServer.Protocol.SOCKS4;
		MqttConnectOptions options = options(broker.getServerURI(), false,
				socks4 ? "wronguser" : "proxyuser", socks4 ? null : "wrong");
		MqttAsyncClient client = new MqttAsyncClient(options);
		try {
			IMqttToken token = client.connect();
			try {
				token.waitForCompletion(10_000);
				Assert.fail("connect should fail when the proxy refuses the login");
			} catch (MqttException expected) {
				// the proxy refused the credentials
			}
			Assert.assertFalse(client.isConnected());
			Assert.assertEquals(Arrays.asList(socks4 ? "wronguser" : "proxyuser"), proxy.getRejectedLogins());
			Assert.assertTrue("no tunnel should be opened", proxy.getTunnels().isEmpty());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * When the proxy cannot be reached, the connect fails, over plain TCP and TLS; the client
	 * never falls back to connecting to the broker directly.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testUnreachableProxyFailsConnect() throws Exception {
		int deadPort;
		try (ServerSocket socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
			deadPort = socket.getLocalPort();   // free once closed, so nothing listens there
		}
		URI[] brokers = {broker.getServerURI(), broker.getSslServerURI()};
		for (URI brokerURI : brokers) {
			MqttConnectOptions options = TestClients.options(brokerURI, name.getMethodName());
			if (brokerURI.getScheme().equals("mqtts")) {
				options.setSslContext(TestTls.clientContext());
			}
			setProxy(options, deadPort, null, null);
			MqttAsyncClient client = new MqttAsyncClient(options);
			try {
				IMqttToken token = client.connect();
				try {
					token.waitForCompletion(10_000);
					Assert.fail(brokerURI.getScheme() + ": connect should fail when the proxy is unreachable");
				} catch (MqttException expected) {
					// connection to the proxy refused
				}
				Assert.assertFalse(client.isConnected());
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	// -------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------

	/**
	 * Connects through the proxy without proxy authentication, sends a QoS 1 message to
	 * itself, and checks that the proxy tunnelled the connection to the broker.
	 *
	 * @param brokerURI the broker URI
	 * @param tls       whether the broker listener uses TLS
	 * @throws Exception if the test fails
	 */
	private void roundTrip(URI brokerURI, boolean tls) throws Exception {
		roundTrip(brokerURI, tls, null, null);
	}

	/**
	 * Connects through the proxy, sends a QoS 1 message to itself, and checks that the proxy
	 * tunnelled the connection to the broker.
	 *
	 * @param brokerURI the broker URI
	 * @param tls       whether the broker listener uses TLS
	 * @param username  the proxy username or SOCKS4 user ID, or {@code null}
	 * @param password  the proxy password, or {@code null}
	 * @throws Exception if the test fails
	 */
	private void roundTrip(URI brokerURI, boolean tls, String username, String password) throws Exception {
		MqttConnectOptions options = options(brokerURI, tls, username, password);
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttAsyncClient client = new MqttAsyncClient(options);
		try {
			client.setCallback(receiver);
			client.connect().waitForCompletion(10_000);
			Assert.assertTrue("not connected through the proxy", client.isConnected());

			String topic = topicPrefix + protocol + "/" + name.getMethodName();
			client.subscribe(topic, 1).waitForCompletion(5000);
			byte[] payload = ("via " + protocol + ": " + brokerURI.getScheme()).getBytes(StandardCharsets.UTF_8);
			TestClients.publish(client, topic, payload, 1, false).waitForCompletion(5000);
			Assert.assertTrue("message not received", receiver.validateReceipt(topic, 1, payload));

			Assert.assertEquals("expected one tunnel to the broker", 1, proxy.getTunnels().size());
			Assert.assertTrue("tunnel did not go to the broker port: " + proxy.getTunnels(),
					proxy.getTunnels().get(0).endsWith(":" + brokerURI.getPort()));
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Returns options for a connection through the test proxy.
	 *
	 * @param brokerURI the broker URI
	 * @param tls       whether to set the test TLS context
	 * @param username  the proxy username or SOCKS4 user ID, or {@code null}
	 * @param password  the proxy password, or {@code null}
	 * @return new options
	 * @throws Exception if the TLS context cannot be created
	 */
	private MqttConnectOptions options(URI brokerURI, boolean tls, String username, String password) throws Exception {
		MqttConnectOptions options = TestClients.options(brokerURI, name.getMethodName());
		if (tls) {
			options.setSslContext(TestTls.clientContext());
		}
		setProxy(options, proxy.getPort(), username, password);
		return options;
	}

	/**
	 * Configures a proxy of the test's protocol on the loopback address.
	 *
	 * @param options  the options to configure
	 * @param port     the proxy port
	 * @param username the proxy username or SOCKS4 user ID, or {@code null}
	 * @param password the proxy password, or {@code null}
	 */
	private void setProxy(MqttConnectOptions options, int port, String username, String password) {
		switch (protocol) {
			case SOCKS4:
				options.setProxy("127.0.0.1", port, username, null, false);
				break;
			case SOCKS5:
				options.setProxy("127.0.0.1", port, username, password, true);
				break;
			default:
				options.setHTTPProxy("127.0.0.1", port, username, password, false);
				break;
		}
	}

	/**
	 * Returns a URI with a different scheme.
	 *
	 * @param uri    the URI
	 * @param scheme the new scheme
	 * @return the URI with the new scheme
	 * @throws Exception if the URI cannot be built
	 */
	private static URI withScheme(URI uri, String scheme) throws Exception {
		return new URI(scheme, null, uri.getHost(), uri.getPort(), uri.getPath(), null, null);
	}
}
