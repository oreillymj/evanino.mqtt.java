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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Every overload of every {@link MqttCallback} event is called: the Paho name and each
 * {@code on<Event>} overload. Not part of Paho's suite. The first tests raise events directly;
 * the rest raise them through a real connection and require a broker; see
 * {@link MosquittoBroker}.
 */
public class CallbackEventsTest {

	/** The broker used by the connection tests. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID and topic. */
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
		topicPrefix = "CallbackEventsTest-" + UUID.randomUUID() + "-";
	}

	/**
	 * The Paho {@code connectComplete} and all three {@code onConnectComplete} overloads are called,
	 * in that order, with the event's arguments.
	 */
	@Test
	public void testConnectCompleteCallsEveryOverload() {
		RecordingCallback cb = new RecordingCallback();
		MqttCallbackEvents.connectComplete(cb, true, false, "tcp://host:1883", "id");
		Assert.assertEquals(Arrays.asList(
				"connectComplete(true, tcp://host:1883)",
				"onConnectComplete(true, tcp://host:1883)",
				"onConnectComplete(true, false, tcp://host:1883)",
				"onConnectComplete(true, false, tcp://host:1883, <uuid>)"), cb.events());
	}

	/**
	 * Every remaining event calls each of its overloads, Paho name first.
	 *
	 * @throws Exception if raising an event fails unexpectedly
	 */
	@Test
	public void testOtherEventsCallEveryOverload() throws Exception {
		RecordingCallback cb = new RecordingCallback();
		Throwable cause = new Exception("gone");
		MqttMessage message = new MqttMessage("hello".getBytes(StandardCharsets.UTF_8));
		IMqttDeliveryToken token = null; // not inspected

		MqttCallbackEvents.connectionLost(cb, cause, "uri", "id");
		MqttCallbackEvents.connectFailed(cb, "uri", "refused", "id");
		MqttCallbackEvents.connecting(cb, true, "uri", "id");
		MqttCallbackEvents.deliveryComplete(cb, token);
		MqttCallbackEvents.messageArrived(cb, "t", message);
		MqttCallbackEvents.reconnectFailed(cb, "uri", "max attempts", "id");
		MqttCallbackEvents.successfulReconnect(cb, true, "uri", "id");

		Assert.assertEquals(Arrays.asList(
				"connectionLost(gone)",
				"onConnectionLost(gone)",
				"onConnectionLost(gone, id)",
				"onConnectionLost(gone, uri, id)",
				"onConnectFailed(uri, refused)",
				"onConnectFailed(uri, refused, id)",
				"onConnecting(true, uri)",
				"onConnecting(true, uri, <uuid>)",
				"deliveryComplete",
				"onDeliveryComplete",
				"messageArrived(t)",
				"onMessageArrived(t)",
				"onMessageArrived(t, hello)",
				"onReconnectFailed(uri, max attempts)",
				"onReconnectFailed(uri, max attempts, id)",
				"onSuccessfulReconnect(true, uri)",
				"onSuccessfulReconnect(true, uri, id)"), cb.events());
	}

	/**
	 * A callback that overrides only the Paho methods (plus the abstract ones it must) still
	 * has the Paho methods called.
	 *
	 * @throws Exception if raising an event fails unexpectedly
	 */
	@Test
	public void testPahoOnlyCallbackIsCalled() throws Exception {
		List<String> calls = new ArrayList<>();
		MqttCallback paho = new MqttCallbackAdapter() {
			@Override
			public void connectComplete(boolean reconnect, String serverURI) {
				calls.add("connectComplete");
			}

			@Override
			public void connectionLost(Throwable cause) {
				calls.add("connectionLost");
			}

			@Override
			public void messageArrived(String topic, MqttMessage message) throws Exception {
				calls.add("messageArrived");
			}

			@Override
			public void deliveryComplete(IMqttDeliveryToken token) {
				calls.add("deliveryComplete");
			}
		};
		MqttCallbackEvents.connectComplete(paho, false, true, "uri", "id");
		MqttCallbackEvents.connectionLost(paho, new Exception(), "uri", "id");
		MqttCallbackEvents.messageArrived(paho, "t", new MqttMessage());
		MqttCallbackEvents.deliveryComplete(paho, null);
		Assert.assertEquals(Arrays.asList("connectComplete", "connectionLost", "messageArrived", "deliveryComplete"), calls);
	}

	/**
	 * A Paho callback migrated by adding {@code on} to each method name, plus the non-Paho
	 * abstract handlers, implements the interface, and each renamed method is called.
	 *
	 * @throws Exception if raising an event fails unexpectedly
	 */
	@Test
	public void testPahoMethodsRenamedWithOnAreCalled() throws Exception {
		List<String> calls = new ArrayList<>();
		MqttCallback migrated = new MqttCallback() {
			@Override
			public void onConnectFailed(String serverURI, String reason) {
			}

			@Override
			public void onConnecting(boolean connecting, String serverURI) {
			}

			@Override
			public void onReconnectFailed(String serverURI, String reason) {
			}

			@Override
			public void onSuccessfulReconnect(boolean isCleanSession, String serverURI) {
			}

			// Paho's four methods, with "on" added to each name
			@Override
			public void onConnectComplete(boolean reconnect, String serverURI) {
				calls.add("onConnectComplete");
			}

			@Override
			public void onConnectionLost(Throwable cause) {
				calls.add("onConnectionLost");
			}

			@Override
			public void onMessageArrived(String topic, MqttMessage message) throws Exception {
				calls.add("onMessageArrived");
			}

			@Override
			public void onDeliveryComplete(IMqttDeliveryToken token) {
				calls.add("onDeliveryComplete");
			}
		};
		MqttCallbackEvents.connectComplete(migrated, false, true, "uri", "id");
		MqttCallbackEvents.connectionLost(migrated, new Exception(), "uri", "id");
		MqttCallbackEvents.messageArrived(migrated, "t", new MqttMessage());
		MqttCallbackEvents.deliveryComplete(migrated, null);
		Assert.assertEquals(Arrays.asList("onConnectComplete", "onConnectionLost", "onMessageArrived", "onDeliveryComplete"), calls);
	}

	/**
	 * A callback written for earlier versions of this client, overriding the names those
	 * versions required (without {@code @Override}, as such code compiles against the current
	 * interface), has every one of them called.
	 *
	 * @throws Exception if raising an event fails unexpectedly
	 */
	@Test
	public void testEarlierVersionNamesAreCalled() throws Exception {
		List<String> calls = new ArrayList<>();
		MqttCallback legacy = new MqttCallbackAdapter() {
			public void connectComplete(boolean reconnect, boolean isCleanSession, String serverURI) {
				calls.add("connectComplete(" + reconnect + ", " + isCleanSession + ", " + serverURI + ")");
			}

			public void connectComplete(boolean reconnect, boolean isCleanSession, String serverURI, String uuid) {
				calls.add("connectComplete(" + reconnect + ", " + isCleanSession + ", " + serverURI + ", " + uuid + ")");
			}

			public void connectionLost(Throwable cause, String uuid) {
				calls.add("connectionLost(" + cause.getMessage() + ", " + uuid + ")");
			}

			public void connectionLost(Throwable cause, String serverURI, String uuid) {
				calls.add("connectionLost(" + cause.getMessage() + ", " + serverURI + ", " + uuid + ")");
			}

			public void isConnecting(String serverURI) {
				calls.add("isConnecting(" + serverURI + ")");
			}

			public void isConnecting(String serverURI, String uuid) {
				calls.add("isConnecting(" + serverURI + ", " + uuid + ")");
			}

			public void onConnectFailed(String serverURI) {
				calls.add("onConnectFailed(" + serverURI + ")");
			}

			public void onMessage(String topic, MqttMessage message) {
				calls.add("onMessage(" + topic + ", " + message.getPayloadString() + ")");
			}

			public void onMessage(String topic, String payload) {
				calls.add("onMessage(" + topic + ", \"" + payload + "\")");
			}
		};
		MqttCallbackEvents.connectComplete(legacy, false, true, "uri", "id");
		MqttCallbackEvents.connectionLost(legacy, new Exception("gone"), "uri", "id");
		MqttCallbackEvents.connecting(legacy, true, "uri", "id");
		MqttCallbackEvents.connectFailed(legacy, "uri", "refused", "id");
		MqttCallbackEvents.messageArrived(legacy, "t", new MqttMessage("hello".getBytes(StandardCharsets.UTF_8)));
		Assert.assertEquals(Arrays.asList(
				"connectComplete(false, true, uri)",
				"connectComplete(false, true, uri, id)",
				"connectionLost(gone, id)",
				"connectionLost(gone, uri, id)",
				"isConnecting(uri)",
				"isConnecting(uri, id)",
				"onConnectFailed(uri)",
				"onMessage(t, hello)",
				"onMessage(t, \"hello\")"), calls);
	}

	/**
	 * An exception from one overload does not stop the later overloads; the first exception is
	 * rethrown once they have all run, with later ones suppressed.
	 */
	@Test
	public void testExceptionInOneOverloadDoesNotStopTheOthers() {
		RuntimeException first = new RuntimeException("first");
		RuntimeException second = new RuntimeException("second");
		List<String> calls = new ArrayList<>();
		MqttCallback cb = new MqttCallbackAdapter() {
			@Override
			public void messageArrived(String topic, MqttMessage message) {
				calls.add("messageArrived");
				throw first;
			}

			@Override
			public void onMessageArrived(String topic, MqttMessage message) {
				calls.add("onMessageArrived");
				throw second;
			}

			@Override
			public void onMessageArrived(String topic, String payload) {
				calls.add("onMessageArrived(String)");
			}
		};
		try {
			MqttCallbackEvents.messageArrived(cb, "t", new MqttMessage());
			Assert.fail("expected the first exception to be rethrown");
		} catch (Exception ex) {
			Assert.assertSame(first, ex);
			Assert.assertArrayEquals(new Throwable[]{second}, ex.getSuppressed());
		}
		Assert.assertEquals(Arrays.asList("messageArrived", "onMessageArrived", "onMessageArrived(String)"), calls);
	}

	/**
	 * Connecting raises the connecting event (start, then end) and every connect-complete
	 * overload, all with the client's server URI and a single client UUID.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testConnectRaisesEveryOverload() throws Exception {
		RecordingCallback cb = new RecordingCallback();
		MqttAsyncClient client = TestClients.createMqttAsyncClient(serverURI, name.getMethodName());
		try {
			client.setCallback(cb);
			client.connect().waitForCompletion(5000);
			String uri = client.getServerURI();

			cb.awaitEvent("onConnectComplete(false, true, " + uri + ", <uuid>)");
			cb.awaitEvent("onConnecting(false, " + uri + ", <uuid>)");
			Assert.assertEquals(1, cb.count("connectComplete(false, " + uri + ")"));
			Assert.assertEquals(1, cb.count("onConnectComplete(false, " + uri + ")"));
			Assert.assertEquals(1, cb.count("onConnectComplete(false, true, " + uri + ")"));
			Assert.assertEquals(1, cb.count("onConnectComplete(false, true, " + uri + ", <uuid>)"));
			Assert.assertEquals(1, cb.count("onConnecting(true, " + uri + ")"));
			Assert.assertEquals(1, cb.count("onConnecting(true, " + uri + ", <uuid>)"));
			Assert.assertEquals(1, cb.count("onConnecting(false, " + uri + ")"));
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * A message received through the client-wide callback raises every message-arrived
	 * overload once, and the publish raises every delivery-complete overload once.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testPublishAndReceiveRaiseEveryOverload() throws Exception {
		String topic = topicPrefix + name.getMethodName();
		RecordingCallback cb = new RecordingCallback();
		MqttAsyncClient client = TestClients.createMqttAsyncClient(serverURI, name.getMethodName());
		try {
			client.setCallback(cb);
			client.connect().waitForCompletion(5000);
			client.subscribe(topic, 1).waitForCompletion(5000);
			TestClients.publish(client, topic, "payload".getBytes(StandardCharsets.UTF_8), 1, false).waitForCompletion(5000);

			cb.awaitEvent("onMessageArrived(" + topic + ", payload)");
			cb.awaitEvent("onDeliveryComplete");
			Assert.assertEquals(1, cb.count("messageArrived(" + topic + ")"));
			Assert.assertEquals(1, cb.count("onMessageArrived(" + topic + ")"));
			Assert.assertEquals(1, cb.count("onMessageArrived(" + topic + ", payload)"));
			Assert.assertEquals(1, cb.count("deliveryComplete"));
			Assert.assertEquals(1, cb.count("onDeliveryComplete"));
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Losing the connection (another client takes over the client ID) raises every
	 * connection-lost overload.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testConnectionLostRaisesEveryOverload() throws Exception {
		RecordingCallback cb = new RecordingCallback();
		MqttAsyncClient client = TestClients.createMqttAsyncClient(serverURI, name.getMethodName());
		MqttAsyncClient usurper = TestClients.createMqttAsyncClient(serverURI, name.getMethodName());
		try {
			client.setCallback(cb);
			client.connect().waitForCompletion(5000);
			String uri = client.getServerURI();
			cb.awaitEvent("onConnectComplete(false, true, " + uri + ")");

			usurper.connect().waitForCompletion(5000);

			cb.awaitEvent("connectionLost");
			cb.awaitEvent("onConnectionLost");
			cb.awaitEvent("onConnectionLost(<uuid>)");
			cb.awaitEvent("onConnectionLost(" + uri + ", <uuid>)");
		} finally {
			TestClients.shutdownQuietly(usurper);
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * A connection attempt to a port nothing listens on raises every connect-failed overload.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testConnectFailureRaisesEveryOverload() throws Exception {
		RecordingCallback cb = new RecordingCallback();
		MqttAsyncClient client = TestClients.createMqttAsyncClient(new URI("tcp://localhost:1"), name.getMethodName());
		try {
			client.setCallback(cb);
			IMqttToken token = client.connect();
			try {
				token.waitForCompletion(5000);
			} catch (MqttException expected) {
				// the connection is refused
			}
			cb.awaitEvent("onConnectFailed");
			cb.awaitEvent("onConnectFailed(<uuid>)");
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	// -------------------------------------------------------------
	// Helper classes
	// -------------------------------------------------------------

	/**
	 * Records every overload of every event as a string. The connection-lost and
	 * connect-failed overloads record only the arguments that are predictable in a test.
	 */
	static class RecordingCallback implements MqttCallback {
		private final List<String> events = new ArrayList<>();
		/** The client UUID passed to the first UUID overload called. */
		private String firstUuid;

		/**
		 * Returns a copy of the events recorded so far.
		 *
		 * @return the events, in order
		 */
		synchronized List<String> events() {
			return new ArrayList<>(events);
		}

		/**
		 * Returns how many times an event has been recorded.
		 *
		 * @param event the event
		 * @return the number of times it was recorded
		 */
		synchronized int count(String event) {
			int n = 0;
			for (String e : events) {
				if (e.equals(event)) {
					n++;
				}
			}
			return n;
		}

		/**
		 * Waits up to ten seconds for an event to be recorded.
		 *
		 * @param event the event
		 * @throws InterruptedException if interrupted while waiting
		 */
		synchronized void awaitEvent(String event) throws InterruptedException {
			long deadline = System.currentTimeMillis() + 10_000;
			while (!events.contains(event)) {
				long remaining = deadline - System.currentTimeMillis();
				if (remaining <= 0) {
					Assert.fail("event not raised: " + event + "; raised: " + events);
				}
				wait(remaining);
			}
		}

		/**
		 * Records an event and wakes any waiter.
		 *
		 * @param event the event
		 */
		private synchronized void record(String event) {
			events.add(event);
			notifyAll();
		}

		/**
		 * Returns {@code <uuid>} for the client UUID passed to the first UUID overload called,
		 * and the value itself for anything else, so tests can check that one non-null UUID is
		 * passed throughout.
		 *
		 * @param id the client UUID passed to an overload
		 * @return the value to record
		 */
		private synchronized String uuid(String id) {
			if (firstUuid == null) {
				firstUuid = id;
			}
			return id != null && id.equals(firstUuid) ? "<uuid>" : "uuid=" + id;
		}

		@Override
		public void connectComplete(boolean reconnect, String serverURI) {
			record("connectComplete(" + reconnect + ", " + serverURI + ")");
		}

		@Override
		public void connectionLost(Throwable cause) {
			record(cause.getMessage() != null && cause.getMessage().equals("gone") ? "connectionLost(gone)" : "connectionLost");
		}

		@Override
		public void deliveryComplete(IMqttDeliveryToken token) {
			record("deliveryComplete");
		}

		@Override
		public void messageArrived(String topic, MqttMessage message) {
			record("messageArrived(" + topic + ")");
		}

		@Override
		public void onConnectComplete(boolean reconnect, String serverURI) {
			record("onConnectComplete(" + reconnect + ", " + serverURI + ")");
		}

		@Override
		public void onConnectComplete(boolean reconnect, boolean isCleanSession, String serverURI) {
			record("onConnectComplete(" + reconnect + ", " + isCleanSession + ", " + serverURI + ")");
		}

		@Override
		public void onConnectComplete(boolean reconnect, boolean isCleanSession, String serverURI, String uuid) {
			record("onConnectComplete(" + reconnect + ", " + isCleanSession + ", " + serverURI + ", " + uuid(uuid) + ")");
		}

		@Override
		public void onConnectFailed(String serverURI, String reason) {
			record("refused".equals(reason) ? "onConnectFailed(" + serverURI + ", " + reason + ")" : "onConnectFailed");
		}

		@Override
		public void onConnectFailed(String serverURI, String reason, String uuid) {
			record("refused".equals(reason)
					? "onConnectFailed(" + serverURI + ", " + reason + ", " + uuid + ")"
					: "onConnectFailed(" + uuid(uuid) + ")");
		}

		@Override
		public void onConnecting(boolean connecting, String serverURI) {
			record("onConnecting(" + connecting + ", " + serverURI + ")");
		}

		@Override
		public void onConnecting(boolean connecting, String serverURI, String uuid) {
			record("onConnecting(" + connecting + ", " + serverURI + ", " + uuid(uuid) + ")");
		}

		@Override
		public void onConnectionLost(Throwable cause) {
			record("gone".equals(cause.getMessage()) ? "onConnectionLost(gone)" : "onConnectionLost");
		}

		@Override
		public void onConnectionLost(Throwable cause, String uuid) {
			record("gone".equals(cause.getMessage())
					? "onConnectionLost(gone, " + uuid + ")"
					: "onConnectionLost(" + uuid(uuid) + ")");
		}

		@Override
		public void onConnectionLost(Throwable cause, String serverURI, String uuid) {
			record("gone".equals(cause.getMessage())
					? "onConnectionLost(gone, " + serverURI + ", " + uuid + ")"
					: "onConnectionLost(" + serverURI + ", " + uuid(uuid) + ")");
		}

		@Override
		public void onDeliveryComplete(IMqttDeliveryToken token) {
			record("onDeliveryComplete");
		}

		@Override
		public void onMessageArrived(String topic, MqttMessage message) {
			record("onMessageArrived(" + topic + ")");
		}

		@Override
		public void onMessageArrived(String topic, String payload) {
			record("onMessageArrived(" + topic + ", " + payload + ")");
		}

		@Override
		public void onReconnectFailed(String serverURI, String reason) {
			record("onReconnectFailed(" + serverURI + ", " + reason + ")");
		}

		@Override
		public void onReconnectFailed(String serverURI, String reason, String uuid) {
			record("onReconnectFailed(" + serverURI + ", " + reason + ", " + uuid + ")");
		}

		@Override
		public void onSuccessfulReconnect(boolean isCleanSession, String serverURI) {
			record("onSuccessfulReconnect(" + isCleanSession + ", " + serverURI + ")");
		}

		@Override
		public void onSuccessfulReconnect(boolean isCleanSession, String serverURI, String uuid) {
			record("onSuccessfulReconnect(" + isCleanSession + ", " + serverURI + ", " + uuid + ")");
		}
	}
}
