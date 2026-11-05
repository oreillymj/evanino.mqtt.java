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
    /**
     * Paho-compatible delivery token. Extends {@link IMqttToken} with access to the message
     * being delivered; it is returned by publish operations only (subscribe, unsubscribe and
     * connect return a plain {@link IMqttToken}).
     */
    public interface IMqttDeliveryToken extends IMqttToken {

        /**
         * Returns the message associated with this delivery token.
         *
         * @return the message associated with this delivery token.
         * @throws MqttException if the message is no longer available
         * (Paho's contract allows implementations to release the payload
         * after delivery completes, at which point this may throw).
         */
        MqttMessage getMessage() throws MqttException;
    }