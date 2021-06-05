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

import java.time.Duration;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import org.springframework.core.log.LogAccessor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.retry.backoff.Sleeper;
import org.springframework.util.Assert;


/**
 *
 * Adjusts timing by creating a thread that will
 * wakeup the consumer from polling, considering that, if consumption is paused,
 * it will check for consumption resuming in increments of 'pollTimeout'. This works best
 * if the consumer is handling a single partition.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 * @see KafkaConsumerBackoffManager
 */
public class WakingKafkaConsumerTimingAdjuster implements KafkaConsumerTimingAdjuster {

	private static final LogAccessor LOGGER =
			new LogAccessor(LogFactory.getLog(WakingKafkaConsumerTimingAdjuster.class));

	private static final long HUNDRED = 100L;

	private static final Duration DEFAULT_TIMING_ADJUSTMENT_THRESHOLD = Duration.ofMillis(HUNDRED);

	private static final int DEFAULT_POLL_TIMEOUTS_FOR_ADJUSTMENT_WINDOW = 2;

	private Duration timingAdjustmentThreshold = DEFAULT_TIMING_ADJUSTMENT_THRESHOLD;

	private int pollTimeoutsForAdjustmentWindow = DEFAULT_POLL_TIMEOUTS_FOR_ADJUSTMENT_WINDOW;

	private final TaskExecutor timingAdjustmentTaskExecutor;

	private final Sleeper sleeper;

	public WakingKafkaConsumerTimingAdjuster(TaskExecutor timingAdjustmentTaskExecutor, Sleeper sleeper) {
		Assert.notNull(timingAdjustmentTaskExecutor, "Task executor cannot be null.");
		Assert.notNull(sleeper, "Sleeper cannot be null.");
		this.timingAdjustmentTaskExecutor = timingAdjustmentTaskExecutor;
		this.sleeper = sleeper;
	}

	public WakingKafkaConsumerTimingAdjuster(TaskExecutor timingAdjustmentTaskExecutor) {
		Assert.notNull(timingAdjustmentTaskExecutor, "Task executor cannot be null.");
		this.timingAdjustmentTaskExecutor = timingAdjustmentTaskExecutor;
		this.sleeper = Thread::sleep;
	}

	/**
	 *
	 * Sets how many pollTimeouts prior to the dueTimeout the adjustment will take place.
	 * Default is 2.
	 *
	 * @param pollTimeoutsForAdjustmentWindow the amount of pollTimeouts in the adjustment window.
	 */
	public void setPollTimeoutsForAdjustmentWindow(int pollTimeoutsForAdjustmentWindow) {
		this.pollTimeoutsForAdjustmentWindow = pollTimeoutsForAdjustmentWindow;
	}

	/**
	 *
	 * Sets the threshold for the timing adjustment to take place. If the time difference between
	 * the probable instant the message will be consumed and the instant it should is lower than
	 * this value, no adjustment will be applied.
	 * Default is 100ms.
	 *
	 * @param timingAdjustmentThreshold the threshold to be set.
	 */
	public void setTimingAdjustmentThreshold(Duration timingAdjustmentThreshold) {
		this.timingAdjustmentThreshold = timingAdjustmentThreshold;
	}

	/**
	 * Adjusts the timing with the provided parameters.
	 *
	 * @param consumerToAdjust the {@link Consumer} that will be adjusted
	 * @param topicPartition the {@link TopicPartition} that will be adjusted
	 * @param pollTimeout the pollConfiguration for the consumer's container
	 * @param timeUntilDue the amount of time until the message is due for consumption
	 * @return the adjusted amount in milliseconds
	 */
	public long adjustTiming(Consumer<?, ?> consumerToAdjust, TopicPartition topicPartition,
							long pollTimeout, long timeUntilDue) {

		boolean isInAdjustmentWindow = timeUntilDue > pollTimeout && timeUntilDue <=
				pollTimeout * this.pollTimeoutsForAdjustmentWindow;

		long adjustmentAmount = timeUntilDue % pollTimeout;
		if (isInAdjustmentWindow && adjustmentAmount > this.timingAdjustmentThreshold.toMillis()) {
			this.timingAdjustmentTaskExecutor.execute(() ->
					doApplyTimingAdjustment(consumerToAdjust, topicPartition, adjustmentAmount));
			return adjustmentAmount;
		}
		return 0L;
	}

	private void doApplyTimingAdjustment(Consumer<?, ?> consumerForTimingAdjustment,
										TopicPartition topicPartition, long adjustmentAmount) {
		try {
			LOGGER.debug(() -> String.format("Applying timing adjustment of %s millis for TopicPartition %s",
					adjustmentAmount, topicPartition));
			this.sleeper.sleep(adjustmentAmount);
			LOGGER.debug(() -> "Waking up consumer for partition topic: " + topicPartition);
			consumerForTimingAdjustment.wakeup();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted waking up consumer while applying timing adjustment " +
					"for TopicPartition " + topicPartition, e);
		}
		catch (Exception e) { // NOSONAR
			LOGGER.error(e, () -> "Error waking up consumer while applying timing adjustment " +
					"for TopicPartition " + topicPartition);
		}
	}
}
