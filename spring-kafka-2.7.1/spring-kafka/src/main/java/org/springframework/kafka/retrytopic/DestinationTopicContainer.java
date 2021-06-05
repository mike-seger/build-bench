/*
 * Copyright 2017-2021 the original author or authors.
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

package org.springframework.kafka.retrytopic;

import java.util.List;

/**
 *
 * Provides methods to store and retrieve {@link DestinationTopic} instances.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 */
public interface DestinationTopicContainer {

	/**
	 * Adds the provided destination topics to the container.
	 * @param destinationTopics the {@link DestinationTopic} list to add.
	 */
	void addDestinationTopics(List<DestinationTopic> destinationTopics);

	/**
	 * Returns the DestinationTopic instance registered for that topic.
	 * @param topicName the topic name of the DestinationTopic to be returned.
	 * @return the DestinationTopic instance registered for that topic.
	 */
	DestinationTopic getDestinationTopicByName(String topicName);

}
