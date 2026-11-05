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

/**
 * The connect token reports the broker's session-present flag, and each client keeps its own
 * keep-alive ping times. Not part of Paho's suite. Requires a broker; see
 * {@link MosquittoBroker}.
 */
public class SessionPresentAndKeepAliveTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID. */
	@Rule
	public final TestName name = new TestName();

	private static URI serverURI;

	/**
	 * Picks up the broker URI.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverURI = broker.getServerURI();
	}

	/**
	 * With {@code cleanSession=false}, the first connection finds no session on the broker
	 * and the second resumes it; with {@code cleanSession=true} no session is ever present.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testConnectTokenReportsSessionPresent() throws Exception {
		String id = name.getMethodName();
		MqttAsyncClient client = new MqttAsyncClient(TestClients.options(serverURI, id));
		try {
			// discard any session left by an earlier run
			IMqttToken token = client.connect(options(id, true));
			token.waitForCompletion(5000);
			Assert.assertFalse("clean session must not report a session", token.getSessionPresent());
			client.disconnect().waitForCompletion(5000);

			token = client.connect(options(id, false));
			token.waitForCompletion(5000);
			Assert.assertFalse("first persistent connection has no session to resume", token.getSessionPresent());
			client.disconnect().waitForCompletion(5000);

			token = client.connect(options(id, false));
			token.waitForCompletion(5000);
			Assert.assertTrue("second persistent connection should resume the session", token.getSessionPresent());
			client.disconnect().waitForCompletion(5000);

			// remove the persistent session from the broker
			client.connect(options(id, true)).waitForCompletion(5000);
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * The session-present flag is available from inside the connect token's action listener.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSessionPresentIsSetBeforeListenerRuns() throws Exception {
		String id = name.getMethodName();
		MqttAsyncClient client = new MqttAsyncClient(TestClients.options(serverURI, id));
		try {
			client.connect(options(id, false)).waitForCompletion(5000);
			client.disconnect().waitForCompletion(5000);

			final boolean[] seen = new boolean[2];   // [0] listener ran, [1] session present
			IMqttToken token = client.connect(options(id, false), null, new IMqttActionListener() {
				@Override
				public void onSuccess(IMqttToken asyncActionToken) {
					synchronized (seen) {
						seen[1] = asyncActionToken.getSessionPresent();
						seen[0] = true;
						seen.notifyAll();
					}
				}

				@Override
				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
				}
			});
			token.waitForCompletion(5000);
			synchronized (seen) {
				long deadline = System.currentTimeMillis() + 5000;
				while (!seen[0] && System.currentTimeMillis() < deadline) {
					seen.wait(deadline - System.currentTimeMillis());
				}
			}
			Assert.assertTrue("action listener was not called", seen[0]);
			Assert.assertTrue("listener should see the resumed session", seen[1]);
			client.disconnect().waitForCompletion(5000);

			client.connect(options(id, true)).waitForCompletion(5000);
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Tokens for actions other than connect never report a session.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testNonConnectTokenHasNoSessionPresent() throws Exception {
		MqttAsyncClient client = TestClients.createMqttAsyncClient(serverURI, name.getMethodName());
		try {
			client.connect().waitForCompletion(5000);
			IMqttToken token = client.subscribe("SessionPresentAndKeepAliveTest/" + name.getMethodName(), 1);
			token.waitForCompletion(5000);
			Assert.assertFalse(token.getSessionPresent());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Each client has its own keep-alive tracker, so a ping recorded by one client does not
	 * change another's ping times.
	 */
	@Test
	public void testKeepAliveTrackerIsPerClient() {
		MqttAsyncClient a = new MqttAsyncClient(TestClients.options(serverURI, name.getMethodName() + "-a"));
		MqttAsyncClient b = new MqttAsyncClient(TestClients.options(serverURI, name.getMethodName() + "-b"));
		Assert.assertNotSame(a.getKeepAliveTracker(), b.getKeepAliveTracker());

		long before = b.getKeepAliveTracker().getPingEpoch();
		a.getKeepAliveTracker().setPingEpoch(before + 60_000);
		Assert.assertEquals(before, b.getKeepAliveTracker().getPingEpoch());
		Assert.assertEquals(before + 60_000, a.getKeepAliveTracker().getPingEpoch());
	}

	/**
	 * Returns connection options for the test broker.
	 *
	 * @param id           the client identifier
	 * @param cleanSession whether to start a clean session
	 * @return new options
	 */
	private static MqttConnectOptions options(String id, boolean cleanSession) {
		MqttConnectOptions options = TestClients.options(serverURI, id);
		options.setCleanSession(cleanSession);
		return options;
	}
}
