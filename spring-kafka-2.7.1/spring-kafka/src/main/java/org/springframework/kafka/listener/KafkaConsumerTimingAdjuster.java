/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/**
 *
 * Adjusts the consumption timing of the given consumer to try to have it consume the
 * next message at a given time until due. Since the {@link org.apache.kafka.clients.consumer.KafkaConsumer}
 * executes on a single thread, this is done in a best-effort basis.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 * @see KafkaConsumerBackoffManager
 */
public interface KafkaConsumerTimingAdjuster {

	/**
	 * Executes the timing adjustment.
	 *
	 * @param consumerToAdjust the consumer that will have consumption adjusted
	 * @param topicPartitionToAdjust the consumer's topic partition to be adjusted
	 * @param containerPollTimeout the consumer's container pollTimeout property
	 * @param timeUntilNextMessageIsDue the time when the next message should be consumed
	 *
	 * @return the applied adjustment amount
	 */
	long adjustTiming(Consumer<?, ?> consumerToAdjust, TopicPartition topicPartitionToAdjust,
							long containerPollTimeout, long timeUntilNextMessageIsDue);
}
