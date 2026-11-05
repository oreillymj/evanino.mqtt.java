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

package bench;

import io.github.oreillymj.evanino.mqtt.IMqttActionListener;
import io.github.oreillymj.evanino.mqtt.IMqttDeliveryToken;
import io.github.oreillymj.evanino.mqtt.IMqttToken;
import io.github.oreillymj.evanino.mqtt.MqttAsyncClient;
import io.github.oreillymj.evanino.mqtt.MqttCallback;
import io.github.oreillymj.evanino.mqtt.MqttConnectOptions;
import io.github.oreillymj.evanino.mqtt.MqttException;
import io.github.oreillymj.evanino.mqtt.MqttMessage;

import java.util.function.Consumer;

/**
 * {@link BenchClient} on this project's {@code MqttAsyncClient}, with the default shared event
 * loop. The system properties {@code bench.flushConsolidation} and {@code bench.sharedGroupSizing}
 * turn on the matching options, which are off by default.
 */
final class NettyBenchClient implements BenchClient {

	/** Whether to enable {@code MqttConnectOptions.enableFlushConsolidation}. */
	private static final boolean FLUSH_CONSOLIDATION = Boolean.getBoolean("bench.flushConsolidation");

	static {
		// Before the first client is made, which creates the shared group.
		MqttAsyncClient.enableSharedGroupSizing(Boolean.getBoolean("bench.sharedGroupSizing"));
	}

	private static final long TIMEOUT_MILLIS = 30_000;

	private MqttAsyncClient client;

	@Override
	public void connect(Settings s) throws Exception {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setServerURI(s.serverURI);
		options.setClientId(s.clientId);
		options.setCleanSession(s.cleanSession);
		options.setAutomaticReconnect(s.automaticReconnect);
		if (s.automaticReconnect) {
			options.setMaxRetries(-1);   // retry for ever, as Paho does
		}
		options.setConnectionTimeout(s.connectionTimeout);
		options.setKeepAliveInterval(s.keepAlive);
		if (s.maxInflight > 0) {
			options.setMaxInflight(s.maxInflight);
		}
		options.enableFlushConsolidation(FLUSH_CONSOLIDATION);
		client = new MqttAsyncClient(options);
		client.setCallback(new MqttCallback() {
			@Override
			public void onConnectComplete(boolean reconnect, String serverURI) {
				if (reconnect) {
					s.onReconnect.run();
				}
			}

			@Override
			public void onConnectFailed(String serverURI, String reason) {
			}

			@Override
			public void onConnecting(boolean connecting, String serverURI) {
			}

			@Override
			public void onConnectionLost(Throwable cause) {
				s.onConnectionLost.run();
			}

			@Override
			public void onDeliveryComplete(IMqttDeliveryToken token) {
			}

			@Override
			public void onMessageArrived(String topic, MqttMessage message) {
			}

			@Override
			public void onReconnectFailed(String serverURI, String reason) {
			}

			@Override
			public void onSuccessfulReconnect(boolean isCleanSession, String serverURI) {
			}
		});
		client.connect().waitForCompletion(TIMEOUT_MILLIS);
	}

	@Override
	public void subscribe(String topic, int qos, MessageHandler handler) throws Exception {
		client.subscribe(topic, qos, (t, m) -> handler.onMessage(t, m.getPayload())).waitForCompletion(TIMEOUT_MILLIS);
	}

	@Override
	public boolean publish(String topic, byte[] payload, int qos, Runnable onComplete, Consumer<Throwable> onFailure) throws Exception {
		try {
			client.publish(topic, payload, qos, false, null, new IMqttActionListener() {
				@Override
				public void onSuccess(IMqttToken token) {
					onComplete.run();
				}

				@Override
				public void onFailure(IMqttToken token, Throwable exception) {
					onFailure.accept(exception);
				}
			});
			return true;
		} catch (MqttException e) {
			if (e.getReasonCode() == MqttException.REASON_CODE_MAX_INFLIGHT) {
				return false;
			}
			throw e;
		}
	}

	@Override
	public boolean isConnected() {
		return client != null && client.isConnected();
	}

	@Override
	public void close() throws Exception {
		if (client == null) {
			return;
		}
		try {
			if (client.isConnected()) {
				client.disconnect().waitForCompletion(TIMEOUT_MILLIS);
			}
		} finally {
			client.close(true);
		}
	}
}
