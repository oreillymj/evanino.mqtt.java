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

import java.util.Objects;

/**
 * An active subscription: a topic filter, the QoS it was subscribed at and the callback that
 * receives matching messages.
 */
final class MqttSubscription {

    private final String topic;
    private final String[] filterLevels;
    private final MqttCallback handler;

    private final boolean once;

    private final MqttQoS qos;

    private boolean called;

    /**
     * Creates a subscription. Matching follows MQTT 3.1.1 section 4.7; see
     * {@link #matches(String, String)}.
     *
     * @param topic   the topic filter, which may include wildcards
     * @param handler the callback for matching messages; may be {@code null}
     * @param qos     the QoS the filter was subscribed at
     * @param once    whether the callback should fire only for the first matching message
     * @throws NullPointerException if {@code topic} is {@code null}
     */
    MqttSubscription(String topic, MqttCallback handler, MqttQoS qos, boolean once) {
        if(topic == null){
            throw new NullPointerException("MqttSubscription->topic is null");
        }
        this.topic = topic;
            this.handler = handler;
        this.qos = qos;
        this.once = once;
        this.filterLevels = matchingFilter(topic).split("/", -1);
    }

    /**
     * Returns the topic filter.
     *
     * @return the topic filter as subscribed
     */
    String getTopic() {
        return topic;
    }

    /**
     * Returns the callback for matching messages.
     *
     * @return the callback, or {@code null} if none was given
     */
    public MqttCallback getHandler() {
        return handler;
    }

    /**
     * Returns the QoS the filter was subscribed at.
     *
     * @return the subscription QoS
     */
    MqttQoS getQOS() { return qos;}

    /**
     * Returns whether the callback should fire only for the first matching message.
     *
     * @return {@code true} for a one-shot subscription
     */
    boolean isOnce() {
        return once;
    }


    /**
     * Returns whether the callback has already been invoked.
     *
     * @return {@code true} if the callback has fired
     */
    boolean isCalled() {
        return called;
    }

    /**
     * Checks whether a topic name matches this subscription's filter; see
     * {@link #matches(String, String)}.
     *
     * @param topic the topic name of an incoming message
     * @return {@code true} if the topic matches the filter
     */
    boolean matches(String topic){
        return matches(this.filterLevels, topic);
    }

    /**
     * Checks whether a topic name matches a topic filter, following MQTT 3.1.1 section 4.7:
     * <ul>
     * <li>The filter and the name are compared level by level, the levels being separated by
     * {@code /}. Every other character, including {@code .} and {@code $}, is literal.</li>
     * <li>{@code +} matches exactly one level, which may be empty: {@code a/+/c} matches
     * {@code a//c}.</li>
     * <li>{@code #} matches any number of levels, including none, so {@code sport/#} matches
     * {@code sport} as well as {@code sport/tennis/player1}.</li>
     * <li>A filter starting with a wildcard does not match a topic name starting with
     * {@code $}, such as {@code $SYS/broker/uptime}; such topics need a filter that starts
     * with {@code $} too.</li>
     * </ul>
     *
     * @param filter    the topic filter
     * @param topicName the topic name
     * @return {@code true} if the name matches the filter
     */
    static boolean matches(String filter, String topicName) {
        return matches(matchingFilter(filter).split("/", -1), topicName);
    }

    /**
     * Returns the part of a topic filter that incoming topic names are matched against. For a
     * shared subscription, {@code $share/{group}/{filter}} (MQTT 5 section 4.8.2, which brokers
     * such as mosquitto, EMQX and HiveMQ also accept from MQTT 3.1.1 clients), or EMQX's older
     * {@code $queue/{filter}}, that is the {@code {filter}}: messages arrive on the plain topic.
     * Any other filter is returned as it is.
     *
     * @param filter the topic filter as subscribed
     * @return the filter to match topic names against
     */
    static String matchingFilter(String filter) {
        if (filter.startsWith("$share/")) {
            int end = filter.indexOf('/', "$share/".length());
            if (end > 0 && end < filter.length() - 1) {
                return filter.substring(end + 1);
            }
        } else if (filter.startsWith("$queue/") && filter.length() > "$queue/".length()) {
            return filter.substring("$queue/".length());
        }
        return filter;
    }

    /**
     * Checks whether a topic name matches a topic filter already split into levels.
     *
     * @param filterLevels the topic filter's levels
     * @param topicName    the topic name
     * @return {@code true} if the name matches the filter
     */
    private static boolean matches(String[] filterLevels, String topicName) {
        if (topicName == null) {
            return false;
        }
        String first = filterLevels[0];
        if (topicName.startsWith("$") && (first.equals("+") || first.equals("#"))) {
            return false;
        }
        String[] nameLevels = topicName.split("/", -1);
        for (int i = 0; i < filterLevels.length; i++) {
            String level = filterLevels[i];
            if (level.equals("#")) {
                return true;   // the rest of the name, possibly nothing
            }
            if (i >= nameLevels.length) {
                return false;
            }
            if (!level.equals("+") && !level.equals(nameLevels[i])) {
                return false;
            }
        }
        return filterLevels.length == nameLevels.length;
    }

    /**
     * Two subscriptions are equal if they have the same topic filter, QoS, one-shot flag and
     * callback (both possibly {@code null}).
     *
     * @param o the object to compare with
     * @return {@code true} if the subscriptions are equal
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MqttSubscription that = (MqttSubscription) o;
        return qos == that.qos && once == that.once && topic.equals(that.topic) && Objects.equals(handler, that.handler);
    }

    /**
     * Computes a hash code from the topic filter, callback and one-shot flag, consistent with
     * {@link #equals(Object)}.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + Objects.hashCode(handler);
        result = 31 * result + (once ? 1 : 0);
        return result;
    }

    /**
     * Records whether the callback has been invoked.
     *
     * @param called {@code true} once the callback has fired
     */
    void setCalled(boolean called) {
        this.called = called;
    }
}