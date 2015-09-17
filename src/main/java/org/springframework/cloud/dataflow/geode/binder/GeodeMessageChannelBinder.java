/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.dataflow.geode.binder;

import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.stream.binder.MessageChannelBinderSupport;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

/**
 * @author Patrick Peralta
 */
public class GeodeMessageChannelBinder extends MessageChannelBinderSupport {
	private static final Logger logger = LoggerFactory.getLogger(GeodeMessageChannelBinder.class);

	public GeodeMessageChannelBinder() {
		logger.warn("GeodeMessageChannelBinder");
	}

	private Region<String, Message<?>> createMessageRegion(String name)  {
		Cache cache = new CacheFactory().create();
		RegionFactory<String, Message<?>> factory =
				cache.createRegionFactory();
		return factory.create(name + "-messages");
	}

	@Override
	public void bindConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {
		logger.warn("bindConsumer");
		createMessageRegion(name);
	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {
		logger.warn("bindPubSubConsumer");
	}

	@Override
	public void bindProducer(String name, MessageChannel outboundBindTarget, Properties properties) {
		logger.warn("bindProducer");
	}

	@Override
	public void bindPubSubProducer(String name, MessageChannel outboundBindTarget, Properties properties) {
		logger.warn("bindPubSubProducer");
	}

	@Override
	public void bindRequestor(String name, MessageChannel requests, MessageChannel replies, Properties properties) {
		logger.warn("bindRequestor");
	}

	@Override
	public void bindReplier(String name, MessageChannel requests, MessageChannel replies, Properties properties) {
		logger.warn("bindReplier");
	}
}
