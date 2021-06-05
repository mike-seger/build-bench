/*
 * Copyright 2015-2019 the original author or authors.
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

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.core.log.LogAccessor;
import org.springframework.util.ObjectUtils;

/**
 * The {@link ErrorHandler} implementation for logging purpose.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class LoggingErrorHandler implements ErrorHandler {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(LoggingErrorHandler.class));

	@Override
	public void handle(Exception thrownException, ConsumerRecord<?, ?> record) {
		LOGGER.error(thrownException, () -> "Error while processing: " + ObjectUtils.nullSafeToString(record));
	}

}
