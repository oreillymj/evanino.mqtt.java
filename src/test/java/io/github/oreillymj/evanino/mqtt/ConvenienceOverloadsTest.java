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

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The Paho convenience overloads, which delegate to the full forms: each one reaches the
 * broker with the right QoS and delivers to the right listener. Not part of Paho's suite.
 * Requires a broker; see {@link MosquittoBroker}.
 */
public class ConvenienceOverloadsTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID and topics. */
	@Rule
	public final TestName name = new TestName();

	/** How long to wait for a message that is expected not to arrive. */
	private static final long NO_MESSAGE_WAIT_MILLIS = 1500;

	private static URI serverURI;
	private static String topicPrefix;

	/**
	 * Picks up the broker URI and a unique topic prefix for the run.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverURI = broker.getServerURI();
		topicPrefix = "ConvenienceOverloadsTest-" + UUID.randomUUID() + "-";
	}

	// -------------------------------------------------------------
	// Asynchronous client
	// -------------------------------------------------------------

	/**
	 * {@code connect(userContext, callback)} connects with the current options and passes the
	 * user context to the listener.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testAsyncConnectWithListener() throws Exception {
		MqttAsyncClient client = TestClients.createMqttAsyncClient(serverURI, name.getMethodName());
		try {
			RecordingActionListener listener = new RecordingActionListener();
			IMqttToken token = client.connect("context", listener);
			token.waitForCompletion(5000);
			Assert.assertTrue(client.isConnected());
			listener.assertSucceededWith("context");
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * {@code subscribe(topic, qos, listener)} delivers matching messages to the listener, not
	 * the default callback.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testAsyncSubscribeWithListener() throws Exception {
		String topic = topicPrefix + name.getMethodName();
		MqttV3Receiver defaultCallback = new MqttV3Receiver();
		MqttAsyncClient client = connectAsync(defaultCallback);
		try {
			PerSubscriptionMessageHandlerTest.Listener listener = new PerSubscriptionMessageHandlerTest.Listener();
			IMqttToken token = client.subscribe(topic, 2, listener);
			token.waitForCompletion(5000);
			Assert.assertArrayEquals(new int[]{2}, token.getGrantedQos());

			TestClients.publish(client, topic, payload("one"), 1, false).waitForCompletion(5000);
			MqttMessage received = listener.getNextMessage();
			Assert.assertNotNull("listener did not receive the message", received);
			Assert.assertEquals("one", received.getPayloadString());
			Assert.assertNull("default callback should not receive it", defaultCallback.receiveNext(NO_MESSAGE_WAIT_MILLIS));
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * {@code subscribe(String[], int[], IMqttMessageListener[])} sends one SUBSCRIBE and routes
	 * each topic to its own listener.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testAsyncSubscribeArraysWithListeners() throws Exception {
		String topicA = topicPrefix + name.getMethodName() + "/a";
		String topicB = topicPrefix + name.getMethodName() + "/b";
		MqttAsyncClient client = connectAsync(new MqttV3Receiver());
		try {
			PerSubscriptionMessageHandlerTest.Listener listenerA = new PerSubscriptionMessageHandlerTest.Listener();
			PerSubscriptionMessageHandlerTest.Listener listenerB = new PerSubscriptionMessageHandlerTest.Listener();
			IMqttToken token = client.subscribe(new String[]{topicA, topicB}, new int[]{0, 1},
					new IMqttMessageListener[]{listenerA, listenerB});
			token.waitForCompletion(5000);
			Assert.assertArrayEquals(new int[]{0, 1}, token.getGrantedQos());

			TestClients.publish(client, topicA, payload("a"), 1, false).waitForCompletion(5000);
			TestClients.publish(client, topicB, payload("b"), 1, false).waitForCompletion(5000);
			Assert.assertEquals("a", listenerA.getNextMessage().getPayloadString());
			Assert.assertEquals("b", listenerB.getNextMessage().getPayloadString());
			Assert.assertNull(listenerA.getNextMessageNow());
			Assert.assertNull(listenerB.getNextMessageNow());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * {@code unsubscribe(String[])} removes every listed subscription, so no further messages
	 * arrive on those topics.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testAsyncUnsubscribeArray() throws Exception {
		String topicA = topicPrefix + name.getMethodName() + "/a";
		String topicB = topicPrefix + name.getMethodName() + "/b";
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttAsyncClient client = connectAsync(receiver);
		try {
			client.subscribe(new String[]{topicA, topicB}, new int[]{1, 1}).waitForCompletion(5000);
			client.unsubscribe(new String[]{topicA, topicB}).waitForCompletion(5000);
			Assert.assertFalse(client.getServerSubscriptions().containsKey(topicA));
			Assert.assertFalse(client.getServerSubscriptions().containsKey(topicB));

			TestClients.publish(client, topicA, payload("a"), 1, false).waitForCompletion(5000);
			TestClients.publish(client, topicB, payload("b"), 1, false).waitForCompletion(5000);
			Assert.assertNull("message received after unsubscribing", receiver.receiveNext(NO_MESSAGE_WAIT_MILLIS));
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * {@code publish(topic, byte[], qos, retained, userContext, callback)} publishes at the
	 * given QoS and passes the user context to the listener.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testAsyncPublishBytesWithListener() throws Exception {
		String topic = topicPrefix + name.getMethodName();
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttAsyncClient client = connectAsync(receiver);
		try {
			client.subscribe(topic, 2).waitForCompletion(5000);
			RecordingActionListener listener = new RecordingActionListener();
			IMqttDeliveryToken token = client.publish(topic, payload("bytes"), 2, false, "context", listener);
			token.waitForCompletion(5000);
			listener.assertSucceededWith("context");
			Assert.assertTrue("message not received at QoS 2", receiver.validateReceipt(topic, 2, payload("bytes")));
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * {@code disconnect(quiesceTimeout)} and {@code disconnect(userContext, callback)}
	 * disconnect the client; the second passes the user context to the listener.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testAsyncDisconnectOverloads() throws Exception {
		MqttAsyncClient client = connectAsync(new MqttV3Receiver());
		try {
			client.disconnect(1000).waitForCompletion(5000);
			Assert.assertFalse(client.isConnected());

			client.connect().waitForCompletion(5000);
			RecordingActionListener listener = new RecordingActionListener();
			client.disconnect("context", listener).waitForCompletion(5000);
			Assert.assertFalse(client.isConnected());
			listener.assertSucceededWith("context");
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * {@code disconnectForcibly(disconnectTimeout)} closes the connection.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testAsyncDisconnectForciblyWithTimeout() throws Exception {
		MqttAsyncClient client = connectAsync(new MqttV3Receiver());
		try {
			client.disconnectForcibly(1000).waitForCompletion(5000);
			long deadline = System.currentTimeMillis() + 5000;
			while (client.isConnected() && System.currentTimeMillis() < deadline) {
				Thread.sleep(50);
			}
			Assert.assertFalse(client.isConnected());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * {@code disconnectForcibly(quiesce, disconnect, sendDisconnectPacket)}: with the flag set,
	 * DISCONNECT is sent, so the broker discards the will; without it the connection just
	 * drops and the broker publishes the will.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testAsyncDisconnectForciblySendDisconnectPacket() throws Exception {
		String willTopic = topicPrefix + name.getMethodName() + "/will";
		MqttV3Receiver watcher = new MqttV3Receiver();
		MqttAsyncClient watcherClient = TestClients.createMqttAsyncClient(serverURI, name.getMethodName() + "-watcher");
		MqttAsyncClient polite = withWill(name.getMethodName() + "-polite", willTopic, "polite");
		MqttAsyncClient abrupt = withWill(name.getMethodName() + "-abrupt", willTopic, "abrupt");
		try {
			watcherClient.setCallback(watcher);
			watcherClient.connect().waitForCompletion(5000);
			watcherClient.subscribe(willTopic, 1).waitForCompletion(5000);

			IMqttToken token = polite.disconnectForcibly(0, 1000, true);
			token.waitForCompletion(5000);
			Assert.assertTrue(token.isComplete());
			Assert.assertFalse(polite.isConnected());
			Assert.assertNull("will published although DISCONNECT was sent", watcher.receiveNext(NO_MESSAGE_WAIT_MILLIS));

			abrupt.disconnectForcibly(0, 1000, false).waitForCompletion(5000);
			Assert.assertTrue("will not published after closing without DISCONNECT",
					watcher.validateReceipt(willTopic, 1, payload("abrupt")));
		} finally {
			TestClients.shutdownQuietly(abrupt);
			TestClients.shutdownQuietly(polite);
			TestClients.shutdownQuietly(watcherClient);
		}
	}

	// -------------------------------------------------------------
	// Blocking client
	// -------------------------------------------------------------

	/**
	 * {@code new MqttClient(serverURI, clientId)}, as in Paho, creates a client that connects
	 * to that broker with that client ID.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSyncServerUriClientIdConstructor() throws Exception {
		String topic = topicPrefix + name.getMethodName();
		MqttClient client = new MqttClient(serverURI.toString(), name.getMethodName());
		try {
			Assert.assertEquals(name.getMethodName(), client.getClientId());
			MqttV3Receiver receiver = new MqttV3Receiver();
			client.setCallback(receiver);
			client.connect();
			Assert.assertTrue(client.isConnected());
			client.subscribe(topic, 1);
			client.publish(topic, payload("hello"), 1, false);
			Assert.assertTrue(receiver.validateReceipt(topic, 1, payload("hello")));
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * Both {@code (serverURI, clientId)} constructors check their arguments straight away, as
	 * Paho's do: an invalid broker URI or a {@code null} client ID is rejected before any
	 * connect.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testServerUriClientIdConstructorsValidateArguments() throws Exception {
		String[] badURIs = {"", "  ", "foo://localhost", "tcp://localhost:1883/path", "tcp://bad host:1883"};
		for (String uri : badURIs) {
			try {
				new MqttClient(uri, "id");
				Assert.fail("MqttClient accepted \"" + uri + "\"");
			} catch (IllegalArgumentException expected) {
				// invalid URI
			}
			try {
				new MqttAsyncClient(uri, "id");
				Assert.fail("MqttAsyncClient accepted \"" + uri + "\"");
			} catch (IllegalArgumentException expected) {
				// invalid URI
			}
		}
		try {
			new MqttClient(null, "id");
			Assert.fail("MqttClient accepted a null URI");
		} catch (IllegalArgumentException expected) {
			// null URI
		}
		try {
			new MqttClient(serverURI.toString(), null);
			Assert.fail("MqttClient accepted a null client ID");
		} catch (IllegalArgumentException expected) {
			// null client ID
		}
		// valid addresses, including the aliases, are accepted
		for (String uri : new String[]{"tcp://localhost", "ssl://localhost:8883", "ws://localhost/mqtt", "https://localhost"}) {
			new MqttClient(uri, "id");
		}
	}

	/**
	 * Options changed after the constructor has parsed the URI still take effect.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testOptionsCanBeChangedAfterConstruction() throws Exception {
		MqttClient client = new MqttClient("tcp://localhost:1883", "id");
		client.getOptions().setPort(1999);
		Assert.assertEquals(1999, client.getOptions().getServerURI().getPort());
		// a later URL with a port replaces the earlier setPort
		client.getOptions().setServerURI("ws://localhost:9001");
		Assert.assertEquals(URI.create("ws://localhost:9001/mqtt"), client.getOptions().getServerURI());
	}

	/**
	 * {@code connect(String host)} uses a port written in {@code host} rather than the port
	 * configured earlier, and the configured port when {@code host} has none.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testAsyncConnectHostWithPortOverridesConfiguredPort() throws Exception {
		int deadPort = deadPort();
		MqttConnectOptions options = TestClients.options(new URI("tcp://localhost:" + deadPort), name.getMethodName());
		MqttAsyncClient client = new MqttAsyncClient(options);
		try {
			client.connect("tcp://" + serverURI.getHost() + ":" + serverURI.getPort()).waitForCompletion(5000);
			Assert.assertTrue("port in host was not used", client.isConnected());
			client.disconnect().waitForCompletion(5000);

			client.getOptions().setPort(serverURI.getPort());
			client.connect(serverURI.getHost()).waitForCompletion(5000);
			Assert.assertTrue("configured port was not kept for a host without a port", client.isConnected());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * {@code connect(host, port)} uses the {@code port} argument even when {@code host}
	 * includes a different port.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testAsyncConnectHostAndPortUsesPortArgument() throws Exception {
		int deadPort = deadPort();
		MqttAsyncClient client = new MqttAsyncClient(TestClients.options(serverURI, name.getMethodName()));
		try {
			client.connect("tcp://" + serverURI.getHost() + ":" + deadPort, serverURI.getPort()).waitForCompletion(5000);
			Assert.assertTrue("port argument was not used", client.isConnected());
			Assert.assertEquals(serverURI.getPort(), client.getOptions().getServerURI().getPort());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * The static {@code generateClientId()}, on both clients as in Paho, returns a new random
	 * identifier each time, short enough for MQTT 3.1, and usable to connect.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testStaticGenerateClientId() throws Exception {
		java.util.Set<String> ids = new java.util.HashSet<>();
		for (int i = 0; i < 1000; i++) {
			String id = (i % 2 == 0) ? MqttClient.generateClientId() : MqttAsyncClient.generateClientId();
			Assert.assertTrue(id, id.matches("netty-mqtt/[A-Za-z0-9]{8}"));
			Assert.assertTrue(id, id.length() <= MqttConnectOptions.MQTT_3_1_MAX_CLIENT_ID_LENGTH);
			ids.add(id);
		}
		Assert.assertEquals("generated identifiers should not repeat", 1000, ids.size());

		String id = MqttClient.generateClientId();
		MqttClient client = new MqttClient(serverURI.toString(), id);
		try {
			client.connect();
			Assert.assertTrue(client.isConnected());
			Assert.assertEquals("getClientId() returns the ID in use", id, client.getClientId());
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * {@code connect()} connects with the client's current options.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSyncConnectWithCurrentOptions() throws Exception {
		MqttClient client = TestClients.createMqttClient(serverURI, name.getMethodName());
		try {
			client.connect();
			Assert.assertTrue(client.isConnected());
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * {@code subscribe(topic)} and {@code subscribe(String[])} subscribe at QoS 1, delivering
	 * to the default callback.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSyncSubscribeDefaultsToQos1() throws Exception {
		String single = topicPrefix + name.getMethodName() + "/single";
		String arrayA = topicPrefix + name.getMethodName() + "/a";
		String arrayB = topicPrefix + name.getMethodName() + "/b";
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttClient client = connectSync(receiver);
		try {
			client.subscribe(single);
			client.subscribe(new String[]{arrayA, arrayB});
			Assert.assertEquals(Integer.valueOf(1), ((MqttAsyncClient) client.getAsyncClient()).getServerSubscriptions().get(single));
			Assert.assertEquals(Integer.valueOf(1), ((MqttAsyncClient) client.getAsyncClient()).getServerSubscriptions().get(arrayA));
			Assert.assertEquals(Integer.valueOf(1), ((MqttAsyncClient) client.getAsyncClient()).getServerSubscriptions().get(arrayB));

			// published at QoS 2, so a QoS 1 subscription receives it at QoS 1
			client.publish(single, payload("s"), 2, false);
			client.publish(arrayB, payload("b"), 2, false);
			Assert.assertTrue(receiver.validateReceipt(single, 1, payload("s")));
			Assert.assertTrue(receiver.validateReceipt(arrayB, 1, payload("b")));
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * {@code subscribe(String[], int[])} subscribes each topic at its own QoS.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSyncSubscribeArraysWithQos() throws Exception {
		String topicA = topicPrefix + name.getMethodName() + "/a";
		String topicB = topicPrefix + name.getMethodName() + "/b";
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttClient client = connectSync(receiver);
		try {
			client.subscribe(new String[]{topicA, topicB}, new int[]{0, 2});
			client.publish(topicA, payload("a"), 2, false);
			Assert.assertTrue(receiver.validateReceipt(topicA, 0, payload("a")));
			client.publish(topicB, payload("b"), 2, false);
			Assert.assertTrue(receiver.validateReceipt(topicB, 2, payload("b")));
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * The four blocking listener overloads deliver to their listeners at the expected QoS:
	 * QoS 1 where none is given.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSyncSubscribeWithListeners() throws Exception {
		String base = topicPrefix + name.getMethodName();
		MqttClient client = connectSync(new MqttV3Receiver());
		try {
			PerSubscriptionMessageHandlerTest.Listener l1 = new PerSubscriptionMessageHandlerTest.Listener();
			PerSubscriptionMessageHandlerTest.Listener l2 = new PerSubscriptionMessageHandlerTest.Listener();
			PerSubscriptionMessageHandlerTest.Listener l3 = new PerSubscriptionMessageHandlerTest.Listener();
			PerSubscriptionMessageHandlerTest.Listener l4 = new PerSubscriptionMessageHandlerTest.Listener();
			client.subscribe(base + "/1", l1);
			client.subscribe(base + "/2", 2, l2);
			client.subscribe(new String[]{base + "/3"}, new IMqttMessageListener[]{l3});
			client.subscribe(new String[]{base + "/4"}, new int[]{0}, new IMqttMessageListener[]{l4});

			for (int i = 1; i <= 4; i++) {
				client.publish(base + "/" + i, payload("m" + i), 2, false);
			}
			assertReceived(l1, "m1", 1);
			assertReceived(l2, "m2", 2);
			assertReceived(l3, "m3", 1);
			assertReceived(l4, "m4", 0);
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * The six added {@code subscribeWithResponse} overloads return completed tokens with the
	 * granted QoS, defaulting to 1.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSyncSubscribeWithResponseOverloads() throws Exception {
		String base = topicPrefix + name.getMethodName();
		MqttClient client = connectSync(new MqttV3Receiver());
		try {
			PerSubscriptionMessageHandlerTest.Listener listener = new PerSubscriptionMessageHandlerTest.Listener();
			assertGranted(client.subscribeWithResponse(base + "/1"), 1);
			assertGranted(client.subscribeWithResponse(base + "/2", listener), 1);
			assertGranted(client.subscribeWithResponse(base + "/3", 2, listener), 2);
			assertGranted(client.subscribeWithResponse(new String[]{base + "/4", base + "/5"}), 1, 1);
			assertGranted(client.subscribeWithResponse(new String[]{base + "/6", base + "/7"}, new int[]{0, 2}), 0, 2);
			assertGranted(client.subscribeWithResponse(new String[]{base + "/8"}, new IMqttMessageListener[]{listener}), 1);

			client.publish(base + "/3", payload("three"), 2, false);
			assertReceived(listener, "three", 2);
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * {@code unsubscribe(String[])} on the blocking client removes every listed subscription.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSyncUnsubscribeArray() throws Exception {
		String topicA = topicPrefix + name.getMethodName() + "/a";
		String topicB = topicPrefix + name.getMethodName() + "/b";
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttClient client = connectSync(receiver);
		try {
			client.subscribe(new String[]{topicA, topicB});
			client.unsubscribe(new String[]{topicA, topicB});
			client.publish(topicA, payload("a"), 1, false);
			client.publish(topicB, payload("b"), 1, false);
			Assert.assertNull("message received after unsubscribing", receiver.receiveNext(NO_MESSAGE_WAIT_MILLIS));
		} finally {
			TestClients.closeQuietly(client);
		}
	}

	// -------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------

	/**
	 * Records the outcome of an action.
	 */
	private static final class RecordingActionListener implements IMqttActionListener {
		private final CountDownLatch done = new CountDownLatch(1);
		private final AtomicReference<Object> context = new AtomicReference<>();
		private volatile boolean succeeded;

		@Override
		public void onSuccess(IMqttToken asyncActionToken) {
			context.set(asyncActionToken.getUserContext());
			succeeded = true;
			done.countDown();
		}

		@Override
		public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
			context.set(asyncActionToken.getUserContext());
			done.countDown();
		}

		/**
		 * Waits for the listener to be called and checks that the action succeeded with the
		 * given user context.
		 *
		 * @param expectedContext the user context passed to the action
		 * @throws InterruptedException if interrupted while waiting
		 */
		void assertSucceededWith(Object expectedContext) throws InterruptedException {
			Assert.assertTrue("action listener was not called", done.await(5, TimeUnit.SECONDS));
			Assert.assertTrue("action failed", succeeded);
			Assert.assertEquals(expectedContext, context.get());
		}
	}

	/**
	 * Connects an asynchronous client with the given default callback.
	 *
	 * @param callback the default callback
	 * @return the connected client
	 * @throws Exception if the connection fails
	 */
	private MqttAsyncClient connectAsync(MqttCallback callback) throws Exception {
		MqttAsyncClient client = TestClients.createMqttAsyncClient(serverURI, name.getMethodName());
		client.setCallback(callback);
		client.connect().waitForCompletion(5000);
		return client;
	}

	/**
	 * Connects an asynchronous client with a will message.
	 *
	 * @param clientId  the client identifier
	 * @param willTopic the will topic
	 * @param will      the will payload
	 * @return the connected client
	 * @throws Exception if the connection fails
	 */
	private static MqttAsyncClient withWill(String clientId, String willTopic, String will) throws Exception {
		MqttConnectOptions options = TestClients.options(serverURI, clientId);
		options.setWill(willTopic, payload(will), 1, false);
		MqttAsyncClient client = new MqttAsyncClient(options);
		client.connect().waitForCompletion(5000);
		return client;
	}

	/**
	 * Connects a blocking client with the given default callback.
	 *
	 * @param callback the default callback
	 * @return the connected client
	 * @throws Exception if the connection fails
	 */
	private MqttClient connectSync(MqttCallback callback) throws Exception {
		MqttClient client = TestClients.createMqttClient(serverURI, name.getMethodName());
		client.setCallback(callback);
		client.connect();
		return client;
	}

	/**
	 * Returns a loopback port with nothing listening on it.
	 *
	 * @return the port
	 * @throws Exception if no port can be found
	 */
	private static int deadPort() throws Exception {
		try (ServerSocket socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
			return socket.getLocalPort();   // free once closed
		}
	}

	/**
	 * Encodes a string payload as UTF-8.
	 *
	 * @param text the payload text
	 * @return the payload bytes
	 */
	private static byte[] payload(String text) {
		return text.getBytes(StandardCharsets.UTF_8);
	}

	/**
	 * Checks that a listener received one message with the given payload and QoS.
	 *
	 * @param listener the listener
	 * @param payload  the expected payload
	 * @param qos      the expected QoS
	 */
	private static void assertReceived(PerSubscriptionMessageHandlerTest.Listener listener, String payload, int qos) {
		MqttMessage message = listener.getNextMessage();
		Assert.assertNotNull("message not received: " + payload, message);
		Assert.assertEquals(payload, message.getPayloadString());
		Assert.assertEquals("QoS of " + payload, qos, message.getQos());
	}

	/**
	 * Checks that a subscribe token completed with the given granted QoS values.
	 *
	 * @param token the subscribe token
	 * @param qos   the expected granted QoS values
	 */
	private static void assertGranted(IMqttToken token, int... qos) {
		Assert.assertTrue(token.isComplete());
		Assert.assertArrayEquals(qos, token.getGrantedQos());
	}
}
