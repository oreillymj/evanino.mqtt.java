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

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link MqttConnectOptions#setRetransmissionInterval(int)}: by default nothing is sent again
 * on a live connection, as in Paho, and a positive interval restores timed resends. Resends on
 * a live connection used to happen after 10 s regardless, and with tens of thousands of QoS 2
 * publishes outstanding they pushed a slow Mosquitto into closing the connection. The broker
 * tests connect through a relay that holds the broker's replies back for
 * {@value #REPLY_DELAY_MILLIS} ms. Not part of Paho's suite; the relay tests need a broker,
 * see {@link MosquittoBroker}.
 */
public class RetransmissionIntervalTest {

	/** The broker used by the relay tests. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID and topic. */
	@Rule
	public final TestName name = new TestName();

	/** How long the relay holds back each reply from the broker. */
	private static final long REPLY_DELAY_MILLIS = 2500;

	private static DelayingRelay relay;
	private static URI relayURI;

	/**
	 * Starts the relay in front of the broker.
	 *
	 * @throws Exception if the relay cannot be started
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		URI serverURI = broker.getServerURI();
		relay = new DelayingRelay(serverURI.getHost(), serverURI.getPort(), REPLY_DELAY_MILLIS);
		relayURI = new URI("tcp://127.0.0.1:" + relay.getPort());
	}

	/**
	 * Stops the relay.
	 */
	@AfterClass
	public static void tearDownAfterClass() {
		if (relay != null) {
			relay.close();
		}
	}

	/**
	 * The interval is 0 by default, and negative values are refused.
	 */
	@Test
	public void testDefaultAndValidation() {
		MqttConnectOptions options = new MqttConnectOptions();
		Assert.assertEquals(0, options.getRetransmissionInterval());
		options.setRetransmissionInterval(10);
		Assert.assertEquals(10, options.getRetransmissionInterval());
		try {
			options.setRetransmissionInterval(-1);
			Assert.fail("a negative interval should be refused");
		} catch (IllegalArgumentException expected) {
			Assert.assertEquals(10, options.getRetransmissionInterval());
		}
	}

	/**
	 * With an interval of 0 the timer never resends, however long the packet waits.
	 */
	@Test(timeout = 10_000)
	public void testNoIntervalNeverResends() {
		Assert.assertEquals(0, resendTimes(0, 600));
	}

	/**
	 * With an interval of 3 s the timer resends after 3 s, and each later delay is 5 s longer:
	 * 8 s more (at 11 s), then 13 s more (at 24 s).
	 */
	@Test(timeout = 10_000)
	public void testIntervalResendsOnSchedule() {
		Assert.assertEquals(0, resendTimes(3, 2));
		Assert.assertEquals(1, resendTimes(3, 3));
		Assert.assertEquals(1, resendTimes(3, 10));
		Assert.assertEquals(2, resendTimes(3, 11));
		Assert.assertEquals(2, resendTimes(3, 23));
		Assert.assertEquals(3, resendTimes(3, 24));
	}

	/**
	 * By default a QoS 1 publish whose PUBACK is slow is not sent again, so the subscriber gets
	 * it once.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSlowAckIsNotResentByDefault() throws Exception {
		Assert.assertEquals(1, deliveriesWithSlowAck(0));
	}

	/**
	 * With an interval of 1 s, a QoS 1 publish whose PUBACK takes 2.5 s is sent again, and the
	 * subscriber gets the duplicate too.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSlowAckIsResentWithInterval() throws Exception {
		Assert.assertTrue(deliveriesWithSlowAck(1) >= 2);
	}

	/**
	 * Starts a retransmission timer with {@code intervalSeconds} on an embedded channel, advances
	 * time by {@code seconds}, and counts the resends.
	 *
	 * @param intervalSeconds the retransmission interval
	 * @param seconds         how long to let the timer run
	 * @return the number of resends
	 */
	private static int resendTimes(int intervalSeconds, int seconds) {
		EmbeddedChannel channel = new EmbeddedChannel();
		try {
			channel.freezeTime();
			AtomicInteger resends = new AtomicInteger();
			RetransmissionHandler<MqttMessage> handler = new RetransmissionHandler<>();
			handler.setOriginalMessage(new MqttMessage(MqttPubRelFixedHeader.getFixedHeader(), MqttMessageIdVariableHeader.from(1)));
			handler.setHandle((MqttFixedHeader header, MqttMessage message) -> resends.incrementAndGet());
			handler.start(channel.eventLoop(), intervalSeconds);
			for (int s = 0; s < seconds; s++) {
				channel.advanceTimeBy(1, TimeUnit.SECONDS);
				channel.runScheduledPendingTasks();
			}
			handler.stop();
			return resends.get();
		} finally {
			channel.finishAndReleaseAll();
		}
	}

	/**
	 * Publishes one QoS 1 message through the relay, whose PUBACK takes
	 * {@value #REPLY_DELAY_MILLIS} ms, and counts how many times a subscriber connected directly
	 * to the broker receives it.
	 *
	 * @param intervalSeconds the publisher's retransmission interval
	 * @return the number of deliveries
	 * @throws Exception if the test fails unexpectedly
	 */
	private int deliveriesWithSlowAck(int intervalSeconds) throws Exception {
		String topic = "RetransmissionIntervalTest/" + name.getMethodName() + "/" + UUID.randomUUID();
		MqttV3Receiver receiver = new MqttV3Receiver();
		MqttAsyncClient subscriber = TestClients.createMqttAsyncClient(broker.getServerURI(), name.getMethodName() + "-sub");
		MqttConnectOptions options = TestClients.options(relayURI, name.getMethodName());
		options.setConnectionTimeout(10);
		options.setRetransmissionInterval(intervalSeconds);
		MqttAsyncClient publisher = new MqttAsyncClient(options);
		try {
			subscriber.setCallback(receiver);
			subscriber.connect().waitForCompletion(5000);
			subscriber.subscribe(topic, 1).waitForCompletion(5000);
			publisher.connect().waitForCompletion(10_000);

			publisher.publish(topic, new byte[]{1}, 1, false).waitForCompletion(10_000);
			Thread.sleep(500);   // let any resend arrive
			int deliveries = 0;
			while (receiver.receiveNext(500) != null) {
				deliveries++;
			}
			return deliveries;
		} finally {
			TestClients.shutdownQuietly(publisher);
			TestClients.shutdownQuietly(subscriber);
		}
	}
}
