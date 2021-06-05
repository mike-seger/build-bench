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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.core.task.TaskExecutor;
import org.springframework.retry.backoff.Sleeper;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
class WakingKafkaConsumerTimingAdjusterTests {

	@Test
	void testAppliesCorrectionIfInCorrectionWindow() throws InterruptedException {
		Sleeper sleeper = mock(Sleeper.class);
		TaskExecutor taskExecutor = mock(TaskExecutor.class);
		Consumer<?, ?> consumer = mock(Consumer.class);
		TopicPartition topicPartition = new TopicPartition("test-topic", 0);
		long pollTimout = 500L;
		long dueBackOffTime = 750L;
		ArgumentCaptor<Runnable> correctionRunnableCaptor = ArgumentCaptor.forClass(Runnable.class);
		WakingKafkaConsumerTimingAdjuster timingAdjuster = new WakingKafkaConsumerTimingAdjuster(taskExecutor, sleeper);
		timingAdjuster.adjustTiming(consumer, topicPartition, pollTimout, dueBackOffTime);
		then(taskExecutor).should(times(1)).execute(correctionRunnableCaptor.capture());
		Runnable correctionRunnable = correctionRunnableCaptor.getValue();
		correctionRunnable.run();
		then(sleeper).should(times(1)).sleep(dueBackOffTime - pollTimout);
		then(consumer).should(times(1)).wakeup();
	}

	@Test
	void testDoesNotApplyCorrectionIfTooLateForCorrectionWindow() {
		Sleeper sleeper = mock(Sleeper.class);
		TaskExecutor taskExecutor = mock(TaskExecutor.class);
		Consumer<?, ?> consumer = mock(Consumer.class);
		TopicPartition topicPartition = new TopicPartition("test-topic", 0);
		long pollTimout = 500L;
		long dueBackOffTime = 250L;
		WakingKafkaConsumerTimingAdjuster timingAdjuster = new WakingKafkaConsumerTimingAdjuster(taskExecutor, sleeper);
		timingAdjuster.adjustTiming(consumer, topicPartition, pollTimout, dueBackOffTime);
		then(taskExecutor).should(never()).execute(any(Runnable.class));
	}

	@Test
	void testDoesNotApplyCorrectionIfTooSoonForCorrectionWindow() {
		Sleeper sleeper = mock(Sleeper.class);
		TaskExecutor taskExecutor = mock(TaskExecutor.class);
		Consumer<?, ?> consumer = mock(Consumer.class);
		TopicPartition topicPartition = new TopicPartition("test-topic", 0);
		long pollTimout = 500L;
		long dueBackOffTime = 1250L;
		WakingKafkaConsumerTimingAdjuster timingAdjuster = new WakingKafkaConsumerTimingAdjuster(taskExecutor, sleeper);
		timingAdjuster.adjustTiming(consumer, topicPartition, pollTimout, dueBackOffTime);
		then(taskExecutor).should(never()).execute(any(Runnable.class));
	}
}
