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

import java.util.concurrent.*;

/**
 * Netty-backed implementation of Paho's IMqttDeliveryToken, returned by publish operations.
 * Wraps a Netty {@link Promise} so it can be driven from Netty's channel-handler thread
 * and observed from the blocking sync API and the async listener API alike.
 */
public class MqttDeliveryToken implements IMqttDeliveryToken, IMqttToken {



    private final IMqttAsyncClient client;
    private final int messageId;          // MQTT packet identifier; -1 / 0 for ops with none (e.g. QoS 0, CONNECT)
    private final String[] topics;

    private final Promise<Void> promise;
    private final Executor callbackExecutor;
    private volatile MqttMessage message;         // set for publish tokens; null otherwise
    private volatile Object userContext;
    private volatile IMqttActionListener actionListener;
    private volatile MqttException exception;
    private volatile boolean sessionPresent;
    private volatile int[] grantedQos;            // set on SUBACK for subscribe tokens


    private SimpleLogger logger = new SimpleLogger();
    private final boolean enableLogging=true;


    /**
     * Creates a delivery token.
     *
     * @param client           the client that initiated the publish
     * @param messageId        the MQTT packet identifier, or -1 for QoS 0
     * @param topics           the topic the message is published to
     * @param promise          the promise tracking the publish
     * @param callbackExecutor the executor that runs the action callback when the promise completes
     */
    public MqttDeliveryToken(IMqttAsyncClient client, int messageId, String[] topics,
                             Promise<Void> promise, Executor callbackExecutor) {
        this.client = client;
        this.messageId = messageId;
        this.topics = topics;
        this.promise = promise;
        this.callbackExecutor = callbackExecutor;

        this.promise.addListener(f -> {
            if (actionListener == null) return;
            if (f.isSuccess()) callbackExecutor.execute(() -> dispatchListener(l -> l.onSuccess(this)));
            else callbackExecutor.execute(() -> dispatchListener(l -> l.onFailure(this, f.cause())));
        });
    }

    /**
     * Logs a message if logging is enabled for this token.
     *
     * @param data the message to log
     */
    private void log(String data){
        if (enableLogging){
            logger.log(data);
        }
    }

    // --- driven internally by the channel handler / registry, not part of IMqttToken ---

    /**
     * Completes the underlying promise successfully, if not already complete.
     */
    void trySuccess() { promise.trySuccess(null); }

    /**
     * Returns the promise tracking the publish.
     *
     * @return the promise
     */
    Promise<Void> getPromise() { return promise; }
    /**
     * Fails the underlying promise, if not already complete.
     *
     * @param cause the reason the publish failed
     */
    void tryFailure(Throwable cause) { promise.tryFailure(cause); }

    /**
     * Sets the message being delivered.
     *
     * @param message the published message
     */
    public void setMessage(MqttMessage message) {
        this.message = message;
    }

    /**
     * {@inheritDoc}
     *
     * @return the published message, or {@code null} if none was set
     */
    @Override
    public MqttMessage getMessage() throws MqttException {
        return message;
    }

    /**
     * {@inheritDoc}
     * <p>
     * A QoS 0 publish has no packet identifier, so its token returns {@code 0}, as in Paho.
     */
    @Override
    public int getMessageId() {
        return Math.max(messageId, 0);   // QoS 0 publishes are created with -1
    }

    /** {@inheritDoc} */
    @Override
    public IMqttAsyncClient getClient() {
        return client;
    }

    /** {@inheritDoc} */
    @Override
    public String[] getTopics() {
        return topics;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isComplete() {
        return promise.isDone();
    }


    /**
     * Marks the publish as delivered by completing the promise. The action callback's
     * {@code onSuccess} is then called once, on the callback executor.
     */
    public void markComplete() {
        trySuccess();
    }

    /**
     * Marks the publish as failed: records the exception and fails the promise. The action
     * callback's {@code onFailure} is then called once, on the callback executor.
     *
     * @param cause the reason delivery failed
     */
    public void markFailed(Throwable cause) {
        this.exception = MqttExceptionUtils.wrapAsMqttException(cause);
        tryFailure(cause);
    }



    /**
     * Records whether the broker resumed an existing session.
     *
     * @param sessionPresent the session-present flag from CONNACK
     */
    public void setSessionPresent(boolean sessionPresent) {
        this.sessionPresent = sessionPresent;
    }

    /**
     * Records the QoS levels granted by the broker.
     *
     * @param grantedQos the granted QoS values
     */
    public void setGrantedQos(int[] grantedQos) {
        this.grantedQos = grantedQos;
    }

    /**
     * Invokes the action callback, if one is set, on the calling thread; it is called from the
     * callback executor. Exceptions thrown by the callback are logged and swallowed.
     *
     * @param invocation the callback method to call
     */
    private void dispatchListener(java.util.function.Consumer<IMqttActionListener> invocation) {
        IMqttActionListener listener = this.actionListener;
        if (listener == null) return;
        try {
            invocation.accept(listener);
        } catch (Exception  ex) {
            // a misbehaving listener must not kill the single shared callback thread
            log("MqttDeliveryToken()->dispatchListener()->exception in IMqttActionListener: " + ex);
        }
    }

    // --- IMqttToken ---

    /**
     * {@inheritDoc}
     * <p>
     * Waits for at most one day.
     */
    @Override
    public void waitForCompletion() throws MqttException {
        waitForCompletion(-1);
    }

    /**
     * {@inheritDoc}
     * <p>
     * A negative timeout waits for at most one day; a timeout of 0 does not wait.
     *
     * @throws MqttException with {@link MqttException#REASON_CODE_CLIENT_TIMEOUT} if the wait
     *                       times out, or the publish failure converted with
     *                       {@link MqttExceptionUtils#wrapAsMqttException(Throwable)}
     */
    @Override
    public void waitForCompletion(long timeout) throws MqttException {
        try {
            boolean done = (timeout < 0)
                    ? promise.await(1, TimeUnit.DAYS)
                    : promise.await(timeout, TimeUnit.MILLISECONDS);
            if (!done) throw new MqttException(MqttException.REASON_CODE_CLIENT_TIMEOUT);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new MqttException(ie);
        }
        if (!promise.isSuccess()) {
            throw MqttExceptionUtils.wrapAsMqttException(promise.cause());
        }
    }



    /**
     * {@inheritDoc}
     * <p>
     * Only failures reported through {@link #markFailed(Throwable)} are returned.
     */
    @Override
    public MqttException getException() {
        return exception;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The listener is only notified if it is set before the token completes; setting it
     * afterwards does not fire it.
     */
    @Override
    public void setActionCallback(IMqttActionListener listener) {
        this.actionListener = listener;
    }

    /** {@inheritDoc} */
    @Override
    public IMqttActionListener getActionCallback() {
        return this.actionListener;
    }



    /** {@inheritDoc} */
    @Override
    public void setUserContext(Object userContext) {
        this.userContext = userContext;
    }

    /** {@inheritDoc} */
    @Override
    public Object getUserContext() {
        return userContext;
    }




    /** {@inheritDoc} */
    @Override
    public int[] getGrantedQos() {
        return grantedQos;
    }

    /** {@inheritDoc} */
    @Override
    public boolean getSessionPresent() {
        return sessionPresent;
    }

    // --- IMqttDeliveryToken ---

    /**
     * Returns the delivery tokens for all of the client's publishes still in flight.
     *
     * @return the pending delivery tokens
     */
    public IMqttDeliveryToken[] getPendingDeliveryTokens() {
        return this.client.getPendingDeliveryTokens();
    }


}
