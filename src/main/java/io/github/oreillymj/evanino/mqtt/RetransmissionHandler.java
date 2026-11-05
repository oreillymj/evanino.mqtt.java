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
 * Based on netty-mqtt-client by JetLinks (https://github.com/jetlinks/netty-mqtt-client),
 * itself based on netty-mqtt by Jeffrey Kog (https://github.com/jeffreykog/netty-mqtt);
 * see NOTICE.
 *
 * Contributors:
 *    Jeffrey Kog - original netty-mqtt implementation
 *    zhouhao (JetLinks) - netty-mqtt-client
 *    Michael OReilly - Paho-compatible API, and changes since
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import io.netty.channel.EventLoop;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Resends an unacknowledged MQTT packet on a timer while the connection is up, the DUP flag set
 * on a PUBLISH. The first resend happens after the interval given to
 * {@link #start(EventLoop, int)} and each later delay is 5 seconds longer; with no interval
 * nothing is resent. An optional retry limit stops retransmission and runs a callback once
 * exceeded.
 *
 * @param <T> the type of packet being retransmitted
 */
final class RetransmissionHandler<T extends MqttMessage> {

    private ScheduledFuture<?> timer;
    private int timeout = 10;
    private BiConsumer<MqttFixedHeader, T> handler;
    private T originalMessage;

    private int maxRetries = -1;       // -1 = infinite (current behavior, unchanged default)
    private int attemptCount = 0;
    private Runnable onMaxRetriesExceeded;
    private boolean running = false;   // guarded by this; false once stop() has been called

    /**
     * Starts retransmission: the first resend after {@code intervalSeconds}, each later one 5
     * seconds later than the last, and the attempt count reset to zero. An interval of 0 or less
     * schedules nothing, so the packet is only sent again after a reconnect (see
     * {@link MqttConnectOptions#setRetransmissionInterval(int)}).
     * {@link #setHandle(BiConsumer)} and {@link #setOriginalMessage(MqttMessage)} must be
     * called first.
     *
     * @param eventLoop       the event loop used to schedule retransmissions
     * @param intervalSeconds the delay before the first resend, or 0 for none
     * @throws NullPointerException if {@code eventLoop} is {@code null} or no handler has been set
     */
    void start(EventLoop eventLoop, int intervalSeconds){
        if(eventLoop == null){
            throw new NullPointerException("start()->eventLoop");
        }
        if(this.handler == null){
            throw new NullPointerException("start()->handler");
        }
        if (intervalSeconds <= 0) {
            return;
        }
        synchronized (this) {
            this.timeout = intervalSeconds;
            this.attemptCount = 0;         // reset per publish attempt cycle
            this.running = true;
        }
        this.startTimer(eventLoop);
    }

    /**
     * Schedules the next retransmission, unless one is already scheduled or retransmission has
     * been stopped. When it fires, it either gives up (if the retry limit is exceeded) or resends
     * the packet with the DUP flag set, increases the delay by 5 seconds and schedules the next
     * retransmission.
     *
     * @param eventLoop the event loop used to schedule the retransmission
     */
    /**
     * Returns the fixed header for sending a packet again. Only a PUBLISH gets the DUP flag:
     * MQTT 3.1.1 fixes the header flags of PUBREC at {@code 0000} and of PUBREL, SUBSCRIBE and
     * UNSUBSCRIBE at {@code 0010} (sections 3.5.1, 3.6.1, 3.8.1 and 3.10.1), and a broker closes
     * the connection on any other value as a malformed packet, so those keep their original
     * flags.
     *
     * @param original the fixed header of the packet as first sent
     * @return the fixed header to send it again with
     */
    static MqttFixedHeader retransmissionHeader(MqttFixedHeader original) {
        boolean dup = original.messageType() == MqttMessageType.PUBLISH || original.isDup();
        return new MqttFixedHeader(original.messageType(), dup, original.qosLevel(), original.isRetain(),
                original.remainingLength());
    }

    private synchronized void startTimer(EventLoop eventLoop){
        if (this.running && this.timer==null) {
            this.timer = eventLoop.schedule(() -> {
                synchronized (this) {
                    if (!running) {
                        return; // stopped while this run was pending
                    }
                    // This run has fired, so clear the handle to let the next one be scheduled.
                    this.timer = null;
                }
                attemptCount++;
                if (maxRetries >= 0 && attemptCount > maxRetries) {
                    stop();
                    if (onMaxRetriesExceeded != null) {
                        onMaxRetriesExceeded.run();
                    }
                    return; // do NOT reschedule
                }
                this.timeout += 5;
                handler.accept(retransmissionHeader(this.originalMessage.fixedHeader()), originalMessage);
                startTimer(eventLoop);
            }, timeout, TimeUnit.SECONDS);
        }
    }

    /**
     * Cancels any scheduled retransmission and prevents further ones until
     * {@link #start(EventLoop, int)} is called again.
     */
    synchronized void stop(){
        this.running = false;
        if(this.timer != null){
            this.timer.cancel(true);
            this.timer = null;
        }
    }

    /**
     * Sets the action that resends the packet.
     *
     * @param runnable receives a copy of the fixed header with the DUP flag set, and the
     *                 original packet
     */
    void setHandle(BiConsumer<MqttFixedHeader, T> runnable) {
        this.handler = runnable;
    }

    /**
     * Sets the maximum number of retransmissions. Equivalent to {@link #setMaxRetries(int)}.
     *
     * @param maxInflightRetries the retry limit, or {@code -1} for no limit
     */
    void setMaxInflightRetries(int maxInflightRetries) { this.maxRetries = maxInflightRetries; }

    /**
     * Sets the packet to retransmit.
     *
     * @param originalMessage the packet originally sent
     */
    void setOriginalMessage(T originalMessage) {
        this.originalMessage = originalMessage;
    }

    /**
     * Sets the maximum number of retransmissions.
     *
     * @param maxRetries the retry limit, or {@code -1} (the default) for no limit
     */
    void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
    /**
     * Sets the callback run when the retry limit is exceeded.
     *
     * @param callback the action to run after retransmission gives up
     */
    void setOnMaxRetriesExceeded(Runnable callback) { this.onMaxRetriesExceeded = callback; }
}