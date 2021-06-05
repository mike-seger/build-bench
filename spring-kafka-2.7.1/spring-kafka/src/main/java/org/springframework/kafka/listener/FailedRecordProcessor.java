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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException;
import org.springframework.util.Assert;
import org.springframework.util.backoff.BackOff;

/**
 * Common super class for classes that deal with failing to consume a consumer record.
 *
 * @author Gary Russell
 * @since 2.3.1
 *
 */
public abstract class FailedRecordProcessor extends KafkaExceptionLogLevelAware implements DeliveryAttemptAware {

	private static final BiPredicate<ConsumerRecord<?, ?>, Exception> ALWAYS_SKIP_PREDICATE = (r, e) -> true;

	private static final BiPredicate<ConsumerRecord<?, ?>, Exception> NEVER_SKIP_PREDICATE = (r, e) -> false;

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR

	private final FailedRecordTracker failureTracker;

	private ExtendedBinaryExceptionClassifier classifier;

	private boolean commitRecovered;

	protected FailedRecordProcessor(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer, BackOff backOff) {
		this.failureTracker = new FailedRecordTracker(recoverer, backOff, this.logger);
		this.classifier = configureDefaultClassifier();
	}

	/**
	 * Return the exception classifier.
	 * @return the classifier.
	 */
	protected BinaryExceptionClassifier getClassifier() {
		return this.classifier;
	}

	/**
	 * Set an exception classifications to determine whether the exception should cause a retry
	 * (until exhaustion) or not. If not, we go straight to the recoverer. By default,
	 * the following exceptions will not be retried:
	 * <ul>
	 * <li>{@link DeserializationException}</li>
	 * <li>{@link MessageConversionException}</li>
	 * <li>{@link MethodArgumentResolutionException}</li>
	 * <li>{@link NoSuchMethodException}</li>
	 * <li>{@link ClassCastException}</li>
	 * </ul>
	 * All others will be retried.
	 * When calling this method, the defaults will not be applied.
	 * @param classifications the classifications.
	 * @param defaultValue whether or not to retry non-matching exceptions.
	 * @see BinaryExceptionClassifier#BinaryExceptionClassifier(Map, boolean)
	 * @see #addNotRetryableExceptions(Class...)
	 */
	public void setClassifications(Map<Class<? extends Throwable>, Boolean> classifications, boolean defaultValue) {
		Assert.notNull(classifications, "'classifications' + cannot be null");
		this.classifier = new ExtendedBinaryExceptionClassifier(classifications, defaultValue);
	}

	/**
	 * Whether the offset for a recovered record should be committed.
	 * @return true to commit recovered record offsets.
	 */
	protected boolean isCommitRecovered() {
		return this.commitRecovered;
	}

	/**
	 * Set to true to commit the offset for a recovered record.
	 * @param commitRecovered true to commit.
	 */
	public void setCommitRecovered(boolean commitRecovered) {
		this.commitRecovered = commitRecovered;
	}

	/**
	 * Set a function to dynamically determine the {@link BackOff} to use, based on the
	 * consumer record and/or exception. If null is returned, the default BackOff will be
	 * used.
	 * @param backOffFunction the function.
	 * @since 2.6
	 */
	public void setBackOffFunction(BiFunction<ConsumerRecord<?, ?>, Exception, BackOff> backOffFunction) {
		this.failureTracker.setBackOffFunction(backOffFunction);
	}

	/**
	 * Set to false to immediately attempt to recover on the next attempt instead
	 * of repeating the BackOff cycle when recovery fails.
	 * @param resetStateOnRecoveryFailure false to retain state.
	 * @since 2.5.5
	 */
	public void setResetStateOnRecoveryFailure(boolean resetStateOnRecoveryFailure) {
		this.failureTracker.setResetStateOnRecoveryFailure(resetStateOnRecoveryFailure);
	}

	/**
	 * Set to true to reset the retry {@link BackOff} if the exception is a different type
	 * to the previous failure for the same record. The
	 * {@link #setBackOffFunction(BiFunction) backOffFunction}, if provided, will be
	 * called to get the {@link BackOff} to use for the new exception; otherwise, the
	 * configured {@link BackOff} will be used.
	 * @param resetStateOnExceptionChange true to reset.
	 * @since 2.6.3
	 */
	public void setResetStateOnExceptionChange(boolean resetStateOnExceptionChange) {
		this.failureTracker.setResetStateOnExceptionChange(resetStateOnExceptionChange);
	}

	/**
	 * Set one or more {@link RetryListener} to receive notifications of retries and
	 * recovery.
	 * @param listeners the listeners.
	 * @since 2.7
	 */
	public void setRetryListeners(RetryListener... listeners) {
		this.failureTracker.setRetryListeners(listeners);
	}

	@Override
	public int deliveryAttempt(TopicPartitionOffset topicPartitionOffset) {
		return this.failureTracker.deliveryAttempt(topicPartitionOffset);
	}

	/**
	 * Add exception types to the default list. By default, the following exceptions will
	 * not be retried:
	 * <ul>
	 * <li>{@link DeserializationException}</li>
	 * <li>{@link MessageConversionException}</li>
	 * <li>{@link ConversionException}</li>
	 * <li>{@link MethodArgumentResolutionException}</li>
	 * <li>{@link NoSuchMethodException}</li>
	 * <li>{@link ClassCastException}</li>
	 * </ul>
	 * All others will be retried.
	 * @param exceptionTypes the exception types.
	 * @since 2.6
	 * @see #removeNotRetryableException(Class)
	 * @see #setClassifications(Map, boolean)
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public final void addNotRetryableExceptions(Class<? extends Exception>... exceptionTypes) {
		Assert.notNull(exceptionTypes, "'exceptionTypes' cannot be null");
		Assert.noNullElements(exceptionTypes, "'exceptionTypes' cannot contain nulls");
		for (Class<? extends Exception> exceptionType : exceptionTypes) {
			Assert.isTrue(Exception.class.isAssignableFrom(exceptionType),
					() -> "exceptionType " + exceptionType + " must be an Exception");
			this.classifier.getClassified().put(exceptionType, false);
		}
	}

	/**
	 * Remove an exception type from the configured list. By default, the following
	 * exceptions will not be retried:
	 * <ul>
	 * <li>{@link DeserializationException}</li>
	 * <li>{@link MessageConversionException}</li>
	 * <li>{@link ConversionException}</li>
	 * <li>{@link MethodArgumentResolutionException}</li>
	 * <li>{@link NoSuchMethodException}</li>
	 * <li>{@link ClassCastException}</li>
	 * </ul>
	 * All others will be retried.
	 * @param exceptionType the exception type.
	 * @return true if the removal was successful.
	 * @see #addNotRetryableExceptions(Class...)
	 * @see #setClassifications(Map, boolean)
	 */
	public boolean removeNotRetryableException(Class<? extends Exception> exceptionType) {
		return this.classifier.getClassified().remove(exceptionType);
	}

	/**
	 * Return a {@link BiPredicate} to call to determine whether the first record in the
	 * list should be skipped.
	 * @param records the records.
	 * @param thrownException the exception.
	 * @return the {@link BiPredicate}.
	 * @deprecated in favor of {@link #getRecoveryStrategy(List, Exception)}.
	 */
	@Deprecated
	protected BiPredicate<ConsumerRecord<?, ?>, Exception> getSkipPredicate(List<ConsumerRecord<?, ?>> records,
			Exception thrownException) {

		if (getClassifier().classify(thrownException)) {
			return this.failureTracker::skip;
		}
		else {
			try {
				this.failureTracker.getRecoverer().accept(records.get(0), thrownException);
			}
			catch (Exception ex) {
				if (records.size() > 0) {
					this.logger.error(ex, () -> "Recovery of record ("
							+ ListenerUtils.recordToString(records.get(0)) + ") failed");
				}
				return NEVER_SKIP_PREDICATE;
			}
			return ALWAYS_SKIP_PREDICATE;
		}
	}

	/**
	 * Return a {@link RecoveryStrategy} to call to determine whether the first record in the
	 * list should be skipped.
	 * @param records the records.
	 * @param thrownException the exception.
	 * @return the {@link RecoveryStrategy}.
	 * @since 2.7
	 */
	protected RecoveryStrategy getRecoveryStrategy(List<ConsumerRecord<?, ?>> records, Exception thrownException) {
		if (getClassifier().classify(thrownException)) {
			return this.failureTracker::recovered;
		}
		else {
			try {
				this.failureTracker.getRecoverer().accept(records.get(0), thrownException);
				this.failureTracker.getRetryListeners().forEach(rl -> rl.recovered(records.get(0), thrownException));
			}
			catch (Exception ex) {
				if (records.size() > 0) {
					this.logger.error(ex, () -> "Recovery of record ("
							+ ListenerUtils.recordToString(records.get(0)) + ") failed");
					this.failureTracker.getRetryListeners().forEach(rl ->
							rl.recoveryFailed(records.get(0), thrownException, ex));
				}
				return (rec, excep, cont, consumer) -> NEVER_SKIP_PREDICATE.test(rec, excep);
			}
			return (rec, excep, cont, consumer) -> ALWAYS_SKIP_PREDICATE.test(rec, excep);
		}
	}

	public void clearThreadState() {
		this.failureTracker.clearThreadState();
	}

	private static ExtendedBinaryExceptionClassifier configureDefaultClassifier() {
		Map<Class<? extends Throwable>, Boolean> classified = new HashMap<>();
		classified.put(DeserializationException.class, false);
		classified.put(MessageConversionException.class, false);
		classified.put(ConversionException.class, false);
		classified.put(MethodArgumentResolutionException.class, false);
		classified.put(NoSuchMethodException.class, false);
		classified.put(ClassCastException.class, false);
		return new ExtendedBinaryExceptionClassifier(classified, true);
	}

	/**
	 * Extended to provide visibility to the current classified exceptions.
	 *
	 * @author Gary Russell
	 *
	 */
	@SuppressWarnings("serial")
	private static final class ExtendedBinaryExceptionClassifier extends BinaryExceptionClassifier {

		ExtendedBinaryExceptionClassifier(Map<Class<? extends Throwable>, Boolean> typeMap, boolean defaultValue) {
			super(typeMap, defaultValue);
			setTraverseCauses(true);
		}

		@Override
		protected Map<Class<? extends Throwable>, Boolean> getClassified() { // NOSONAR worthless override
			return super.getClassified();
		}

	}

}
