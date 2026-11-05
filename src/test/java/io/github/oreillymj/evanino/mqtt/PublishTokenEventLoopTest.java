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

import io.netty.channel.EventLoop;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A publish token is completed on the channel's event loop, and its promise uses that loop, so
 * completing it notifies listeners at once instead of handing them to another event loop of the
 * shared group, which cost a selector wake-up per message and halved QoS 0 throughput. Not part
 * of Paho's suite. Requires a broker; see {@link MosquittoBroker}.
 */
public class PublishTokenEventLoopTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID and topic. */
	@Rule
	public final TestName name = new TestName();

	/**
	 * At QoS 0, 1 and 2, the token's promise notifies its listeners on the event loop of the
	 * channel the publish was sent on.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testPublishTokenCompletesOnChannelEventLoop() throws Exception {
		MqttAsyncClient client = TestClients.createMqttAsyncClient(broker.getServerURI(), name.getMethodName());
		try {
			client.connect().waitForCompletion(5000);
			EventLoop channelLoop = client.getChannel().eventLoop();
			for (int qos = 0; qos <= 2; qos++) {
				MqttDeliveryToken token = (MqttDeliveryToken) client.publish(
						"PublishTokenEventLoopTest/" + name.getMethodName(), new byte[]{1}, qos, false);
				AtomicReference<Thread> notifiedOn = new AtomicReference<>();
				CountDownLatch notified = new CountDownLatch(1);
				token.getPromise().addListener(f -> {
					notifiedOn.set(Thread.currentThread());
					notified.countDown();
				});
				Assert.assertTrue("QoS " + qos + " publish did not complete", notified.await(5, TimeUnit.SECONDS));
				Assert.assertTrue("QoS " + qos + " listener ran on " + notifiedOn.get().getName()
								+ ", not the channel's event loop", channelLoop.inEventLoop(notifiedOn.get()));
			}
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}
}
