/*
 * Copyright 2019-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import java.time.Clock;
import java.time.Instant;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.event.ListenerContainerPartitionIdleEvent;
import org.springframework.kafka.retrytopic.TestClockUtils;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class PartitionPausingBackoffManagerTests {

	@Mock
	private KafkaListenerEndpointRegistry registry;

	@Mock
	private MessageListenerContainer listenerContainer;

	@Mock
	private WakingKafkaConsumerTimingAdjuster timingAdjustmentManager;

	@Mock
	private ListenerContainerPartitionIdleEvent partitionIdleEvent;

	@Mock
	private Consumer<?, ?> consumer;

	@Mock
	private ContainerProperties containerProperties;

	private static final String testListenerId = "testListenerId";

	private static final Clock clock = TestClockUtils.CLOCK;

	private static final String testTopic = "testTopic";

	private static final int testPartition = 0;

	private static final long pollTimeout = 500L;

	private static final TopicPartition topicPartition = new TopicPartition(testTopic, testPartition);

	private static final long originalTimestamp = Instant.now(clock).minusMillis(2500L).toEpochMilli();

	@Test
	void shouldBackOffGivenDueTimestampIsLater() {

		// given
		given(this.registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		PartitionPausingBackoffManager backoffManager =
				new PartitionPausingBackoffManager(registry, timingAdjustmentManager, clock);

		long dueTimestamp = originalTimestamp + 5000;
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp, testListenerId, topicPartition, consumer);

		// then
		KafkaBackoffException backoffException = catchThrowableOfType(() -> backoffManager.backOffIfNecessary(context),
				KafkaBackoffException.class);

		// when
		assertThat(backoffException.getDueTimestamp()).isEqualTo(dueTimestamp);
		assertThat(backoffException.getListenerId()).isEqualTo(testListenerId);
		assertThat(backoffException.getTopicPartition()).isEqualTo(topicPartition);
		assertThat(backoffManager.getBackOffContext(topicPartition)).isEqualTo(context);
		then(listenerContainer).should(times(1)).pausePartition(topicPartition);
	}

	@Test
	void shouldNotBackoffGivenDueTimestampIsPast() {

		// given
		PartitionPausingBackoffManager backoffManager =
				new PartitionPausingBackoffManager(registry, timingAdjustmentManager, clock);
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(originalTimestamp - 5000, testListenerId, topicPartition, consumer);

		// then
		backoffManager.backOffIfNecessary(context);

		// when
		assertThat(backoffManager.getBackOffContext(topicPartition)).isNull();
		then(listenerContainer).should(times(0)).pausePartition(topicPartition);
	}

	@Test
	void shouldDoNothingIfIdleBeforeDueTimestamp() {

		// given
		given(this.partitionIdleEvent.getTopicPartition()).willReturn(topicPartition);
		given(registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(listenerContainer.getContainerProperties()).willReturn(containerProperties);
		given(containerProperties.getPollTimeout()).willReturn(pollTimeout);

		PartitionPausingBackoffManager backoffManager =
				new PartitionPausingBackoffManager(registry, timingAdjustmentManager, clock);

		long dueTimestamp = originalTimestamp + 5000;
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp, testListenerId, topicPartition, consumer);
		backoffManager.addBackoff(context, topicPartition);

		// then
		backoffManager.onApplicationEvent(partitionIdleEvent);

		// when
		assertThat(backoffManager.getBackOffContext(topicPartition)).isEqualTo(context);
		then(timingAdjustmentManager).should(times(1)).adjustTiming(
				consumer, topicPartition, pollTimeout, getTimeUntilDue(dueTimestamp));
		then(listenerContainer).should(times(0)).resumePartition(topicPartition);
	}

	private long getTimeUntilDue(long dueTimestamp) {
		return dueTimestamp - Instant.now(clock).toEpochMilli();
	}

	@Test
	void shouldResumePartitionIfIdleAfterDueTimestamp() {

		// given
		given(registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(listenerContainer.getContainerProperties()).willReturn(containerProperties);
		given(containerProperties.getPollTimeout()).willReturn(500L);

		given(this.registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(this.partitionIdleEvent.getTopicPartition()).willReturn(topicPartition);
		long dueTimestamp = originalTimestamp - 5000;
		given(timingAdjustmentManager
				.adjustTiming(consumer, topicPartition, pollTimeout, getTimeUntilDue(dueTimestamp)))
				.willReturn(0L);
		PartitionPausingBackoffManager backoffManager =
				new PartitionPausingBackoffManager(registry, timingAdjustmentManager, clock);
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp, testListenerId, topicPartition, consumer);
		backoffManager.addBackoff(context, topicPartition);

		// when
		backoffManager.onApplicationEvent(partitionIdleEvent);

		// then
		then(timingAdjustmentManager).should(times(1)).adjustTiming(
				consumer, topicPartition, pollTimeout, getTimeUntilDue(dueTimestamp));
		assertThat(backoffManager.getBackOffContext(topicPartition)).isNull();
		then(listenerContainer).should(times(1)).resumePartition(topicPartition);
	}

	@Test
	void shouldResumePartitionIfCorrectionIsApplied() {

		// given
		given(registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(listenerContainer.getContainerProperties()).willReturn(containerProperties);
		long pollTimeout = 500L;
		given(containerProperties.getPollTimeout()).willReturn(pollTimeout);

		given(this.registry.getListenerContainer(testListenerId)).willReturn(listenerContainer);
		given(this.partitionIdleEvent.getTopicPartition()).willReturn(topicPartition);
		long dueTimestamp = originalTimestamp + 5000;
		given(timingAdjustmentManager
				.adjustTiming(consumer, topicPartition, pollTimeout, getTimeUntilDue(dueTimestamp)))
				.willReturn(1000L);
		PartitionPausingBackoffManager backoffManager =
				new PartitionPausingBackoffManager(registry, timingAdjustmentManager, clock);
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp, testListenerId, topicPartition, consumer);
		backoffManager.addBackoff(context, topicPartition);

		// when
		backoffManager.onApplicationEvent(partitionIdleEvent);

		// then
		then(timingAdjustmentManager).should(times(1)).adjustTiming(
				consumer, topicPartition, pollTimeout, getTimeUntilDue(dueTimestamp));
		then(listenerContainer).should(times(1)).resumePartition(topicPartition);
		assertThat(backoffManager.getBackOffContext(topicPartition)).isNull();
	}
}
