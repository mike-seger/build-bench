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

import java.time.Clock;
import java.time.Instant;

import org.springframework.kafka.KafkaException;

/**
 * A {@link KafkaException} that records the timestamp
 * of when it was thrown.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 */
public class TimestampedException extends KafkaException {

	private static final long serialVersionUID = -2544217643924234282L;

	private final long timestamp;

	public TimestampedException(Exception ex, Clock clock) {
		super(ex.getMessage(), ex);
		this.timestamp = Instant.now(clock).toEpochMilli();
	}

	public TimestampedException(Exception ex) {
		super(ex.getMessage(), ex);
		this.timestamp = Instant.now(Clock.systemDefaultZone()).toEpochMilli();
	}

	public long getTimestamp() {
		return this.timestamp;
	}
}
