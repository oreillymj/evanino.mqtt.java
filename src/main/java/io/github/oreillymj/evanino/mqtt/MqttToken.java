/*******************************************************************************
 * Copyright (c) 2017, 2026 Michael OReilly
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

import io.netty.util.concurrent.Promise;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;


/**
 * {@link IMqttToken} backed by a Netty {@link Promise}. Action callbacks are run on a separate
 * callback executor rather than the event loop, so slow listeners cannot stall I/O.
 *
 * @param <V> the type of result the underlying promise completes with
 */
public class MqttToken<V> implements IMqttToken {
	private final Promise<V> promise; // from your EventLoop
	private volatile IMqttActionListener actionListener;
	private volatile Object userContext;
	private final Executor callbackExecutor; // NOT the event loop
	private volatile String[] topics;
	private volatile int[] grantedQos;
	private volatile IMqttAsyncClient client;
	private volatile int messageId;

	/**
	 * Creates a token that completes when the given promise does.
	 *
	 * @param promise          the promise tracking the action, typically from the channel's event loop
	 * @param callbackExecutor the executor that runs the action callback
	 */
	public MqttToken(Promise<V> promise, Executor callbackExecutor) {
		this.promise = promise;
		this.callbackExecutor = callbackExecutor;
		this.promise.addListener(f -> {
			if (actionListener == null) return;
			if (f.isSuccess()) {
				callbackExecutor.execute(() -> actionListener.onSuccess(this));
			} else {
				callbackExecutor.execute(() -> actionListener.onFailure(this, f.cause()));
			}
		});
	}


	/**
	 * Completes the token successfully, if it has not already completed.
	 *
	 * @param result the result of the action
	 * @return {@code true} if this call completed the token
	 */
	boolean trySuccess(V result) {
		return promise.trySuccess(result);
	}

	/**
	 * Fails the token, if it has not already completed.
	 *
	 * @param cause the reason the action failed
	 * @return {@code true} if this call completed the token
	 */
	boolean tryFailure(Throwable cause) {
		return promise.tryFailure(cause);
	}


	/**
	 * Fails the token, if it has not already completed. The action callback's
	 * {@code onFailure} is then called once, by the promise listener, on the callback executor.
	 *
	 * @param t the reason the action failed
	 */
	void completeExceptionally(Throwable t) {
		promise.tryFailure(t);
	}

	// MqttToken<V>
	/**
	 * Completes {@code follower} with this token's outcome once this token completes. On
	 * success the granted QoS values are copied across as well.
	 *
	 * @param follower the token to complete in step with this one
	 */
	void propagateTo(MqttToken<V> follower) {
		promise.addListener(f -> {
			if (f.isSuccess()) {
				follower.setGrantedQos(this.getGrantedQos()); // copy SUBACK result
				follower.trySuccess(promise.getNow());
			} else {
				follower.tryFailure(f.cause());
			}
		});
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Waits for at most one day.
	 */
	public void waitForCompletion() throws MqttException {
		waitForCompletion(0);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * A timeout of 0 or less waits for at most one day.
	 *
	 * @param timeoutMs maximum time to wait in milliseconds
	 * @throws MqttException with {@link MqttException#REASON_CODE_CLIENT_TIMEOUT} if the wait
	 *                       times out, or the action's failure converted with
	 *                       {@link MqttExceptionUtils#wrapAsMqttException(Throwable)}
	 */
	public void waitForCompletion(long timeoutMs) throws MqttException {
		try {
			boolean done = (timeoutMs <= 0) ? promise.await(1, TimeUnit.DAYS) : promise.await(timeoutMs, TimeUnit.MILLISECONDS);
			if (!done) throw new MqttException(MqttException.REASON_CODE_CLIENT_TIMEOUT);
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			throw new MqttException(ie);
		}
		if (!promise.isSuccess()) throw MqttExceptionUtils.wrapAsMqttException(promise.cause());
	}


	/**
	 * {@inheritDoc}
	 * <p>
	 * The listener is only notified if it is set before the token completes; setting it
	 * afterwards does not fire it.
	 */
	public void setActionCallback(IMqttActionListener l) {
		this.actionListener = l;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttActionListener getActionCallback() {
		return actionListener;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttAsyncClient getClient() {
		return client;
	}

	/**
	 * Sets the client that created this token.
	 *
	 * @param client the client
	 */
	void setClient(IMqttAsyncClient client) {
		this.client = client;
	}

	/** {@inheritDoc} */
	public void setUserContext(Object c) {
		this.userContext = c;
	}

	/** {@inheritDoc} */
	public Object getUserContext() {
		return userContext;
	}

	/**
	 * Sets the topics associated with the action.
	 *
	 * @param t the topic strings
	 */
	public void setTopics(String[] t) {
		this.topics = t;
	}

	/** {@inheritDoc} */
	public String[] getTopics() {
		return topics;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * For a subscribe or unsubscribe token, this is the packet identifier of the SUBSCRIBE or
	 * UNSUBSCRIBE sent; a token that follows a SUBSCRIBE already in flight reports that
	 * packet's identifier. It is {@code 0} for connect and disconnect tokens, and when no
	 * packet was needed, for example when the topic was already subscribed.
	 */
	@Override
	public int getMessageId() {
		return messageId;
	}

	/**
	 * Sets the packet identifier of the packet sent for this action.
	 *
	 * @param messageId the packet identifier
	 */
	void setMessageId(int messageId) {
		this.messageId = messageId;
	}

	/**
	 * Sets the QoS levels granted by the broker in its SUBACK.
	 *
	 * @param grantedQos the granted QoS values, in subscription order
	 */
	void setGrantedQos(int[] grantedQos) {
		this.grantedQos = grantedQos;
	}

	/** {@inheritDoc} */
	public int[] getGrantedQos() {
		return grantedQos;
	} // add to IMqttToken interface too

	/**
	 * {@inheritDoc}
	 * <p>
	 * For a connect token, this is the session-present flag from the broker's CONNACK once the
	 * connection has succeeded. It is {@code false} before then, if the connection failed, and
	 * for tokens of any other action.
	 */
	@Override
	public boolean getSessionPresent() {
		V result = promise.isSuccess() ? promise.getNow() : null;
		return result instanceof MqttConnectResult && ((MqttConnectResult) result).isServerSessionPresent();
	}

	/** {@inheritDoc} */
	public boolean isComplete() {
		return promise.isDone();
	}

	/** {@inheritDoc} */
	public MqttException getException() {
		return promise.isDone() && !promise.isSuccess() ? MqttExceptionUtils.wrapAsMqttException(promise.cause()) : null;
	}

}//class




