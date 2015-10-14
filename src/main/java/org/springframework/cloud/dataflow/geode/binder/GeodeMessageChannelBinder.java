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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.stream.binder.MessageChannelBinderSupport;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;

/**
 * @author Patrick Peralta
 */
public class GeodeMessageChannelBinder extends MessageChannelBinderSupport {
	private static final Logger logger = LoggerFactory.getLogger(GeodeMessageChannelBinder.class);

	public GeodeMessageChannelBinder() {
		logger.warn("GeodeMessageChannelBinder");
	}

	private Region<Long, Message<?>> createMessageRegion(String name)  {
		Properties properties = new Properties();
		properties.put("locators", "localhost[7777]");
		properties.put("log-level", "warning");
		Cache cache = new CacheFactory(properties).create();
		RegionFactory<Long, Message<?>> factory =
				cache.createRegionFactory(RegionShortcut.PARTITION);
		return factory.create(name + "-messages");
	}

	@Override
	public void bindConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {
		logger.warn("bindConsumer({})", name);
		Executors.newSingleThreadExecutor().submit(new QueueReader(createMessageRegion(name), inboundBindTarget));
	}

	@Override
	public void bindPubSubConsumer(String name, MessageChannel inboundBindTarget, Properties properties) {
		logger.warn("bindPubSubConsumer");
	}

	@Override
	public void bindProducer(String name, MessageChannel outboundBindTarget, Properties properties) {
		logger.warn("bindProducer({})", name);
		Assert.isInstanceOf(SubscribableChannel.class, outboundBindTarget);

		((SubscribableChannel) outboundBindTarget).subscribe(new SendingHandler(createMessageRegion(name)));
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

	private static class QueueReader implements Runnable {

		private final Region<Long, Message<?>> messageRegion;

		private final MessageChannel messageChannel;

		public QueueReader(Region<Long, Message<?>> messageRegion, MessageChannel messageChannel) {
			this.messageRegion = messageRegion;
			this.messageChannel = messageChannel;
		}

		@Override
		public void run() {
			Region<Long, Message<?>> localMessageRegion =
					PartitionRegionHelper.getLocalData(this.messageRegion);

			while (true) {
				try {
					List<Long> keys = new ArrayList<>(localMessageRegion.keySet());
					logger.debug("fetched {} messages", keys.size());
					Collections.sort(keys);
					Map<Long, Message<?>> messages = localMessageRegion.getAll(keys);
					for (Long key : keys) {
						Message<?> message = messages.get(key);
						logger.debug("QueueReader({})", message);
						messageChannel.send(message);
					}
					// todo: if message processing fails, the messages should
					// remain in the region
					localMessageRegion.removeAll(keys);

					Thread.sleep(1000);
				}
				catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static class SendingHandler implements MessageHandler {

		private final Region<Long, Message<?>> messageRegion;

		private final AtomicLong sequence = new AtomicLong();

		public SendingHandler(Region<Long, Message<?>> messageRegion) {
			this.messageRegion = messageRegion;
		}

		@Override
		public void handleMessage(Message<?> message) throws MessagingException {
			logger.debug("publishing message {}", message);
			messageRegion.put(sequence.getAndIncrement(), message);
		}
	}
}
