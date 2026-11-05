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

import io.netty.handler.codec.mqtt.MqttQoS;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * The "last will and testament" message the broker publishes on the client's behalf if the
 * client disconnects ungracefully. Instances are immutable; use {@link #builder()} or the
 * constructor to create one.
 */
@SuppressWarnings({"WeakerAccess", "unused", "SimplifiableIfStatement", "StringBufferReplaceableByString"})
public final class MqttLastWill {

    private final String topic;
    private final byte[] payload;
    private final boolean retain;
    private final MqttQoS qos;

    /**
     * Creates a last will message with a text payload, encoded as UTF-8.
     *
     * @param topic   the topic the will is published to
     * @param message the will payload
     * @param retain  whether the broker should retain the will message
     * @param qos     the QoS the will is published at
     * @throws NullPointerException if {@code topic}, {@code message} or {@code qos} is {@code null}
     * @throws IllegalArgumentException if the topic or the encoded message is more than 65535 bytes
     */
    public MqttLastWill(String topic, String message, boolean retain, MqttQoS qos) {
        this(topic, utf8(message), retain, qos);
    }

    /**
     * Creates a last will message with a binary payload, which is sent exactly as given.
     *
     * @param topic   the topic the will is published to
     * @param payload the will payload; copied
     * @param retain  whether the broker should retain the will message
     * @param qos     the QoS the will is published at
     * @throws NullPointerException if {@code topic}, {@code payload} or {@code qos} is {@code null}
     * @throws IllegalArgumentException if the topic encodes to, or the payload is, more than
     *                                  65535 bytes, the most MQTT can carry in CONNECT
     */
    public MqttLastWill(String topic, byte[] payload, boolean retain, MqttQoS qos) {
        if(topic == null){
            throw new NullPointerException("MqttLastWill()->topic");
        }
        if(payload == null){
            throw new NullPointerException("MqttLastWill()->message");
        }
        if(qos == null){
            throw new NullPointerException("MqttLastWill()->qos");
        }
        MqttStrings.checkLength(topic, "The will topic");
        MqttStrings.checkLength(payload, "The will payload");
        this.topic = topic;
        this.payload = payload.clone();
        this.retain = retain;
        this.qos = qos;
    }

    /**
     * Encodes a text payload as UTF-8.
     *
     * @param message the text, or {@code null}
     * @return the UTF-8 bytes, or {@code null} if {@code message} is {@code null}
     */
    private static byte[] utf8(String message) {
        return (message == null) ? null : message.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Returns the topic the will is published to.
     *
     * @return the will topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Returns the will payload decoded as UTF-8. Use {@link #getPayload()} for a binary payload.
     *
     * @return the will message
     */
    public String getMessage() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    /**
     * Returns the will payload exactly as it is sent.
     *
     * @return a copy of the payload bytes
     */
    public byte[] getPayload() {
        return payload.clone();
    }

    /**
     * Returns whether the broker should retain the will message.
     *
     * @return {@code true} if the will is retained
     */
    public boolean isRetain() {
        return retain;
    }

    /**
     * Returns the QoS the will is published at.
     *
     * @return the will QoS
     */
    public MqttQoS getQos() {
        return qos;
    }

    /**
     * Creates a builder for a last will message.
     *
     * @return a new, empty builder
     */
    public static MqttLastWill.Builder builder(){
        return new MqttLastWill.Builder();
    }

    /**
     * Builder for {@link MqttLastWill}. The topic, message and QoS must be set before
     * calling {@link #build()}.
     */
    public static final class Builder {

        private String topic;
        private String message;
        private boolean retain;
        private MqttQoS qos;

        /**
         * Returns the topic set so far.
         *
         * @return the will topic, or {@code null}
         */
        public String getTopic() {
            return topic;
        }

        /**
         * Sets the topic the will is published to.
         *
         * @param topic the will topic
         * @return this builder
         * @throws NullPointerException if {@code topic} is {@code null}
         */
        public Builder setTopic(String topic) {
            if(topic == null){
                throw new NullPointerException("setTopic()->topic");
            }
            this.topic = topic;
            return this;
        }

        /**
         * Returns the message set so far.
         *
         * @return the will message, or {@code null}
         */
        public String getMessage() {
            return message;
        }

        /**
         * Sets the will payload.
         *
         * @param message the will message
         * @return this builder
         * @throws NullPointerException if {@code message} is {@code null}
         */
        public Builder setMessage(String message) {
            if(message == null){
                throw new NullPointerException("setMessage()->message");
            }
            this.message = message;
            return this;
        }

        /**
         * Returns the retain flag set so far.
         *
         * @return {@code true} if the will is retained
         */
        public boolean isRetain() {
            return retain;
        }

        /**
         * Sets whether the broker should retain the will message.
         *
         * @param retain the retain flag
         * @return this builder
         */
        public Builder setRetain(boolean retain) {
            this.retain = retain;
            return this;
        }

        /**
         * Returns the QoS set so far.
         *
         * @return the will QoS, or {@code null}
         */
        public MqttQoS getQos() {
            return qos;
        }

        /**
         * Sets the QoS the will is published at.
         *
         * @param qos the will QoS
         * @return this builder
         * @throws NullPointerException if {@code qos} is {@code null}
         */
        public Builder setQos(MqttQoS qos) {
            if(qos == null){
                throw new NullPointerException("setQos()->qos");
            }
            this.qos = qos;
            return this;
        }

        /**
         * Creates the last will message.
         *
         * @return a new last will
         * @throws NullPointerException if the topic, message or QoS has not been set
         */
        public MqttLastWill build(){
            return new MqttLastWill(topic, message, retain, qos);
        }
    }

    /**
     * Two last wills are equal if their topic, payload, retain flag and QoS are equal.
     *
     * @param o the object to compare with
     * @return {@code true} if the wills are equal
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MqttLastWill that = (MqttLastWill) o;

        if (retain != that.retain) return false;
        if (!topic.equals(that.topic)) return false;
        if (!Arrays.equals(payload, that.payload)) return false;
        return qos == that.qos;

    }

    /**
     * Computes a hash code consistent with {@link #equals(Object)}.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + Arrays.hashCode(payload);
        result = 31 * result + (retain ? 1 : 0);
        result = 31 * result + qos.hashCode();
        return result;
    }

    /**
     * Describes every field of the will, for debugging.
     *
     * @return a string representation of this will
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MqttLastWill{");
        sb.append("topic='").append(topic).append('\'');
        sb.append(", message='").append(getMessage()).append('\'');
        sb.append(", retain=").append(retain);
        sb.append(", qos=").append(qos.name());
        sb.append('}');
        return sb.toString();
    }
}
