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
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The rules for moving through an {@link MqttServerList} ({@link ServerFailover}), the list's
 * settings, the {@link MqttConnectOptions} copy constructor, and connects that move through a
 * list of brokers that each accept the TCP connection and close it at once, so every attempt
 * fails and is counted. No MQTT broker is needed.
 */
public class ServerFailoverTest {

	private final List<StubServer> stubs = new ArrayList<>();

	/**
	 * Stops the stub servers.
	 */
	@After
	public void stopStubs() {
		for (StubServer stub : stubs) {
			stub.close();
		}
	}

	/**
	 * Returns options for a broker at {@code tcp://localhost:port}.
	 *
	 * @param port       the port
	 * @param maxRetries the broker's retry limit
	 * @return the options
	 */
	private static MqttConnectOptions server(int port, int maxRetries) {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setServerURI("tcp://localhost:" + port);
		options.setMaxRetries(maxRetries);
		options.setRetryInterval(1);
		options.setMaxReconnectDelay(8000);
		return options;
	}

	/**
	 * Returns a list of brokers on ports 1001, 1002, ... with the given retry limit each.
	 *
	 * @param count      the number of brokers
	 * @param maxRetries each broker's retry limit
	 * @return the list
	 */
	private static MqttServerList servers(int count, int maxRetries) {
		MqttServerList list = new MqttServerList();
		for (int i = 0; i < count; i++) {
			list.add(server(1001 + i, maxRetries));
		}
		return list;
	}

	/**
	 * Calls {@link ServerFailover#failed()} until it gives up, and returns the brokers chosen.
	 *
	 * @param failover the failover state
	 * @param max      the most failures to record, to stop a list with no limit
	 * @return the index of the first broker, then of the broker chosen after each failure
	 */
	private static List<Integer> walk(ServerFailover failover, int max) {
		List<Integer> order = new ArrayList<>();
		order.add(failover.currentIndex());
		for (int i = 0; i < max && failover.failed(); i++) {
			order.add(failover.currentIndex());
		}
		return order;
	}

	/**
	 * Next-server mode with round robin moves to the next broker after each failure, wraps round
	 * from the end, and gives up when the failures exceed the retry limit.
	 */
	@Test
	public void testNextServerRoundRobin() {
		MqttServerList list = servers(3, 2).setMaxRetries(4);
		Assert.assertTrue(list.isReconnectUsingNextServer());   // the default
		Assert.assertEquals(Arrays.asList(0, 1, 2, 0, 1), walk(new ServerFailover(list), 100));
	}

	/**
	 * By default the retry limit of next-server mode is the first broker's.
	 */
	@Test
	public void testNextServerUsesFirstServersRetryLimit() {
		MqttServerList list = new MqttServerList(server(1001, 1), server(1002, 5));
		Assert.assertEquals(MqttServerList.RETRIES_OF_FIRST_SERVER, list.getMaxRetries());
		Assert.assertEquals(Arrays.asList(0, 1), walk(new ServerFailover(list), 100));
	}

	/**
	 * With no retry limit, next-server mode never gives up.
	 */
	@Test
	public void testNextServerWithoutLimit() {
		MqttServerList list = servers(2, 2).setMaxRetries(-1);
		Assert.assertEquals(51, walk(new ServerFailover(list), 50).size());
	}

	/**
	 * Random choice never picks the broker that has just failed, prefers brokers that have not
	 * failed since the last connection, and once every broker has failed picks any other.
	 */
	@Test
	public void testRandomExcludesFailedServers() {
		for (long seed = 0; seed < 200; seed++) {
			MqttServerList list = servers(4, 2).setMaxRetries(-1).setStrategy(MqttServerList.Strategy.RANDOM);
			ServerFailover failover = new ServerFailover(list, new Random(seed));
			List<Integer> order = walk(failover, 12);
			// the first four are the four brokers, each once: the failed ones are excluded
			Assert.assertEquals("seed " + seed + ": " + order, new HashSet<>(Arrays.asList(0, 1, 2, 3)),
					new HashSet<>(order.subList(0, 4)));
			for (int i = 1; i < order.size(); i++) {
				Assert.assertNotEquals("seed " + seed + ": " + order, order.get(i - 1), order.get(i));
			}
		}
	}

	/**
	 * Random choice spreads clients over the other brokers rather than sending them all to the
	 * next one.
	 */
	@Test
	public void testRandomSpreadsClients() {
		Set<Integer> chosen = new HashSet<>();
		for (long seed = 0; seed < 100; seed++) {
			MqttServerList list = servers(4, 2).setStrategy(MqttServerList.Strategy.RANDOM);
			ServerFailover failover = new ServerFailover(list, new Random(seed));
			Assert.assertTrue(failover.failed());
			chosen.add(failover.currentIndex());
		}
		Assert.assertEquals(new HashSet<>(Arrays.asList(1, 2, 3)), chosen);
	}

	/**
	 * Until-the-retry-limit mode stays with each broker until it has failed more than its own
	 * retry limit, moves to the next, and gives up when every broker has used up its retries.
	 */
	@Test
	public void testUntilRetryLimit() {
		MqttServerList list = new MqttServerList(server(1001, 1), server(1002, 0), server(1003, 2))
				.reconnectUntilRetryLimit(true);
		Assert.assertFalse(list.isReconnectUsingNextServer());
		Assert.assertEquals(Arrays.asList(0, 0, 1, 2, 2, 2), walk(new ServerFailover(list), 100));
	}

	/**
	 * Until-the-retry-limit mode, after a connection to a broker in the middle of the list is
	 * lost, wraps round to the start before giving up.
	 */
	@Test
	public void testUntilRetryLimitWrapsRound() {
		MqttServerList list = servers(3, 0).reconnectUntilRetryLimit(true);
		ServerFailover failover = new ServerFailover(list);
		Assert.assertTrue(failover.failed());
		failover.connected();   // connected to the second broker
		Assert.assertEquals(Arrays.asList(1, 2, 0), walk(failover, 100));
	}

	/**
	 * Until-the-retry-limit mode never leaves a broker with no retry limit.
	 */
	@Test
	public void testUntilRetryLimitWithoutLimit() {
		MqttServerList list = servers(2, -1).reconnectUntilRetryLimit(true);
		List<Integer> order = walk(new ServerFailover(list), 20);
		Assert.assertEquals(21, order.size());
		Assert.assertEquals(new HashSet<>(Arrays.asList(0)), new HashSet<>(order));
	}

	/**
	 * A successful connection forgets the failures, so the retries start again from there.
	 */
	@Test
	public void testConnectedResetsFailures() {
		MqttServerList list = servers(3, 2).setMaxRetries(2);
		ServerFailover failover = new ServerFailover(list);
		Assert.assertTrue(failover.failed());
		Assert.assertTrue(failover.failed());
		failover.connected();   // connected to the third broker
		Assert.assertEquals(Arrays.asList(2, 0, 1), walk(failover, 100));
	}

	/**
	 * The wait doubles with each failure of a broker in until-the-retry-limit mode, starting
	 * again at a new broker, and with each pass through the list in next-server mode.
	 */
	@Test
	public void testDelays() {
		ServerFailover until = new ServerFailover(servers(2, 3).reconnectUntilRetryLimit(true));
		List<Long> delays = new ArrayList<>();
		for (int i = 0; i < 6 && until.failed(); i++) {
			delays.add(until.delayMillis());
		}
		Assert.assertEquals(Arrays.asList(1000L, 2000L, 4000L, 1000L, 1000L, 2000L), delays);

		ServerFailover next = new ServerFailover(servers(2, 2).setMaxRetries(-1));
		delays.clear();
		for (int i = 0; i < 6 && next.failed(); i++) {
			delays.add(next.delayMillis());
		}
		Assert.assertEquals(Arrays.asList(1000L, 1000L, 2000L, 2000L, 4000L, 4000L), delays);
	}

	/**
	 * The two modes are mutually exclusive: turning either on or off sets the other.
	 */
	@Test
	public void testModesAreExclusive() {
		MqttServerList list = new MqttServerList();
		list.reconnectUntilRetryLimit(true);
		Assert.assertTrue(list.isReconnectUntilRetryLimit());
		Assert.assertFalse(list.isReconnectUsingNextServer());
		list.reconnectUsingNextServer(true);
		Assert.assertFalse(list.isReconnectUntilRetryLimit());
		list.reconnectUsingNextServer(false);
		Assert.assertTrue(list.isReconnectUntilRetryLimit());
		list.reconnectUntilRetryLimit(false);
		Assert.assertTrue(list.isReconnectUsingNextServer());
	}

	/**
	 * {@link MqttServerList#addServer(String)} copies the first broker's options with the new
	 * address; a port in the address wins over one set on the first broker's options, and a
	 * port set on the copy afterwards is used.
	 */
	@Test
	public void testAddServerCopiesFirstOptions() {
		MqttConnectOptions a = new MqttConnectOptions();
		a.setServerURI("tcp://broker-a");
		a.setPort(1884);
		a.setUserName("user");
		a.setPassword("secret".toCharArray());
		a.setClientId("client");
		a.setKeepAliveInterval(17);

		MqttServerList list = new MqttServerList(a);
		MqttConnectOptions b = list.addServer("tcp://broker-b:1999");
		MqttConnectOptions c = list.addServer("tcp://broker-c");
		c.setPort(1885);

		Assert.assertEquals(3, list.size());
		Assert.assertEquals("tcp://broker-b:1999", b.getServerURI().toString());
		Assert.assertEquals("tcp://broker-c:1885", c.getServerURI().toString());
		Assert.assertEquals("tcp://broker-a:1884", a.getServerURI().toString());
		for (MqttConnectOptions copy : Arrays.asList(b, c)) {
			Assert.assertEquals("user", copy.getUserName());
			Assert.assertEquals("secret", new String(copy.getPassword()));
			Assert.assertEquals("client", copy.getClientId());
			Assert.assertEquals(17, copy.getKeepAliveInterval());
		}
	}

	/**
	 * Paho's {@code setServerURIs} becomes a list of copies of the options in next-server mode
	 * with round robin, whose first connect makes one pass through the list.
	 */
	@Test
	public void testFromServerURIs() {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setUserName("user");
		options.setServerURIs(new String[]{"tcp://a:1001", "tcp://b:1002", "tcp://c:1003", "tcp://d:1004"});
		MqttServerList list = MqttServerList.fromServerURIs(options);
		Assert.assertEquals(4, list.size());
		Assert.assertTrue(list.isReconnectUsingNextServer());
		Assert.assertEquals(MqttServerList.Strategy.ROUND_ROBIN, list.getStrategy());
		Assert.assertEquals(MqttServerList.RETRIES_OF_FIRST_SERVER, list.getMaxRetries());
		Assert.assertTrue(list.isOnePassOnFirstConnect());
		for (int i = 0; i < 4; i++) {
			MqttConnectOptions server = list.getServers().get(i);
			Assert.assertEquals(1001 + i, server.getServerURI().getPort());
			Assert.assertEquals("user", server.getUserName());
			Assert.assertNull(server.getServerURIs());
		}
		Assert.assertFalse(new MqttServerList(options).isOnePassOnFirstConnect());
	}

	/**
	 * As in Paho, a first connect through {@code setServerURIs} tries each address once and then
	 * fails, even with no retry limit (as in Paho mode).
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testServerURIsFirstConnectMakesOnePass() throws Exception {
		StubServer a = stub();
		StubServer b = stub();
		StubServer c = stub();
		MqttConnectOptions options = new MqttConnectOptions();
		options.setClientId("failover-test");
		options.setMaxRetries(-1);
		options.setRetryInterval(5);   // would show as a wait if it were used between addresses
		options.setServerURIs(new String[]{
				"tcp://localhost:" + a.port(), "tcp://localhost:" + b.port(), "tcp://localhost:" + c.port()});
		MqttAsyncClient client = new MqttAsyncClient(options);
		try {
			long start = System.nanoTime();
			try {
				client.connect().waitForCompletion(10000);
				Assert.fail("connected to a stub server");
			} catch (MqttException expected) {
				// every address fails
			}
			Assert.assertTrue("waited between addresses",
					System.nanoTime() - start < TimeUnit.SECONDS.toNanos(4));
			Assert.assertEquals(1, a.accepted.get());
			Assert.assertEquals(1, b.accepted.get());
			Assert.assertEquals(1, c.accepted.get());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * The copy constructor copies every field: each one is given a value that differs from the
	 * default, and must have the same value in the copy.
	 *
	 * @throws Exception if reflection fails
	 */
	@Test
	public void testCopyConstructorCopiesEveryField() throws Exception {
		MqttConnectOptions source = new MqttConnectOptions();
		List<Field> fields = new ArrayList<>();
		for (Field field : MqttConnectOptions.class.getDeclaredFields()) {
			if (Modifier.isStatic(field.getModifiers()) || Modifier.isFinal(field.getModifiers())
					|| field.getName().equals("logger")) {
				continue;
			}
			field.setAccessible(true);
			fields.add(field);
			Class<?> type = field.getType();
			Object value;
			if (type == int.class) {
				value = 7000 + fields.size();
			} else if (type == long.class) {
				value = 9000L + fields.size();
			} else if (type == boolean.class) {
				value = !field.getBoolean(source);
			} else if (type == String.class) {
				value = "value-" + field.getName();
			} else if (type == String[].class) {
				value = new String[]{"tcp://a", "tcp://b"};
			} else if (type == Map.class) {
				Map<String, String> map = new HashMap<>();
				map.put("k", "v");
				value = map;
			} else {
				value = field.get(source);   // other objects: shared by the copy, checked below
			}
			field.set(source, value);
		}
		MqttConnectOptions copy = new MqttConnectOptions(source);
		for (Field field : fields) {
			Object expected = field.get(source);
			Object actual = field.get(copy);
			if (expected instanceof String[]) {
				Assert.assertArrayEquals(field.getName(), (String[]) expected, (String[]) actual);
				Assert.assertNotSame(field.getName(), expected, actual);
			} else if (expected instanceof Map) {
				Assert.assertEquals(field.getName(), expected, actual);
				Assert.assertNotSame(field.getName(), expected, actual);
			} else {
				Assert.assertEquals(field.getName(), expected, actual);
			}
		}
	}

	/**
	 * A connect through a list in next-server mode tries the brokers in turn, and fails once
	 * the failures exceed the retry limit: with a limit of 2, three attempts.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testConnectGivesUpAfterRetryLimit() throws Exception {
		StubServer a = stub();
		StubServer b = stub();
		MqttServerList list = new MqttServerList(fast(a, 2), fast(b, 2));
		MqttAsyncClient client = new MqttAsyncClient(fast(a, 2));
		try {
			IMqttToken token = client.connect(list);
			try {
				token.waitForCompletion(10000);
				Assert.fail("connected to a stub server");
			} catch (MqttException expected) {
				// every attempt fails
			}
			Assert.assertEquals(2, a.accepted.get());
			Assert.assertEquals(1, b.accepted.get());
			Assert.assertFalse(client.isConnected());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * A connect through a list in until-the-retry-limit mode uses up each broker's retries in
	 * turn before giving up.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testConnectUntilRetryLimit() throws Exception {
		StubServer a = stub();
		StubServer b = stub();
		MqttServerList list = new MqttServerList(fast(a, 1), fast(b, 2)).reconnectUntilRetryLimit(true);
		MqttAsyncClient client = new MqttAsyncClient(fast(a, 1));
		try {
			try {
				client.connect(list).waitForCompletion(10000);
				Assert.fail("connected to a stub server");
			} catch (MqttException expected) {
				// every attempt fails
			}
			Assert.assertEquals(2, a.accepted.get());
			Assert.assertEquals(3, b.accepted.get());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * {@code disconnect()} stops a connect that is moving through a list, and the connect fails.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testDisconnectStopsConnect() throws Exception {
		StubServer a = stub();
		StubServer b = stub();
		MqttConnectOptions first = fast(a, -1);
		first.setRetryInterval(1);   // a wait between attempts, for disconnect() to land in
		MqttConnectOptions second = new MqttConnectOptions(first);
		second.setServerURI("tcp://localhost:" + b.port());
		MqttAsyncClient client = new MqttAsyncClient(first);
		try {
			IMqttToken token = client.connect(new MqttServerList(first, second));
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (a.accepted.get() + b.accepted.get() < 2 && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			client.disconnect();
			try {
				token.waitForCompletion(5000);
				Assert.fail("the connect did not fail");
			} catch (MqttException expected) {
				Assert.assertEquals(MqttException.REASON_CODE_CLIENT_DISCONNECTING, expected.getReasonCode());
			}
			int attempts = a.accepted.get() + b.accepted.get();
			Thread.sleep(2500);
			Assert.assertEquals("attempts went on after disconnect()", attempts, a.accepted.get() + b.accepted.get());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Returns options for a stub server, with no wait between attempts.
	 *
	 * @param stub       the server
	 * @param maxRetries the retry limit
	 * @return the options
	 */
	private static MqttConnectOptions fast(StubServer stub, int maxRetries) {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setServerURI("tcp://localhost:" + stub.port());
		options.setClientId("failover-test");
		options.setMaxRetries(maxRetries);
		options.setRetryInterval(0);
		return options;
	}

	/**
	 * Starts a stub server.
	 *
	 * @return the server
	 * @throws IOException if it cannot listen
	 */
	private StubServer stub() throws IOException {
		StubServer stub = new StubServer();
		stubs.add(stub);
		return stub;
	}

	/**
	 * A server that accepts connections, counts them, and closes each at once.
	 */
	private static final class StubServer {

		private final ServerSocket socket = new ServerSocket(0);

		final AtomicInteger accepted = new AtomicInteger();

		/**
		 * Starts listening on a free port.
		 *
		 * @throws IOException if it cannot listen
		 */
		StubServer() throws IOException {
			Thread thread = new Thread(() -> {
				while (!socket.isClosed()) {
					try (Socket s = socket.accept()) {
						accepted.incrementAndGet();
					} catch (IOException e) {
						return;
					}
				}
			}, "stub-server-" + socket.getLocalPort());
			thread.setDaemon(true);
			thread.start();
		}

		/**
		 * Returns the port.
		 *
		 * @return the port
		 */
		int port() {
			return socket.getLocalPort();
		}

		/**
		 * Stops listening.
		 */
		void close() {
			try {
				socket.close();
			} catch (IOException ignored) {
				// closing anyway
			}
		}
	}
}
