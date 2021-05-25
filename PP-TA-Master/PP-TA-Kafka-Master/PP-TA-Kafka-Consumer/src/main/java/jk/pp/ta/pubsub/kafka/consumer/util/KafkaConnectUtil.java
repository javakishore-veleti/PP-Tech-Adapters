package jk.pp.ta.pubsub.kafka.consumer.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jk.pp.engg.foundations.common.core.pubsub.PubSubTopic;
import jk.pp.ta.pubsub.kafka.consumer.ConsumerRuntimeContext;
import jk.pp.ta.pubsub.kafka.consumer.KafkaConsumerRunnable;

public class KafkaConnectUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectUtil.class);

	public static Properties createConsumerConfig(PubSubTopic pubSubTopic) throws Exception {

		Properties props = new Properties();

		InputStream iStream = null;
		String fileName = pubSubTopic.getAdapterConsumerConfig();

		try {
			iStream = KafkaConnectUtil.class.getClassLoader().getResourceAsStream(fileName);

			if (iStream == null) {
				throw new Exception("Kafka Consumer configuration file does not exists -> " + fileName);
			}

			props.load(iStream);

			props.put("group.id", pubSubTopic.getConsumerGroupId());

		} catch (IOException exception) {
			throw new Exception(exception);
		} finally {
			if (iStream != null) {
				iStream.close();
			}
		}

		return props;
	}

	public static ConsumerRuntimeContext createConsumerRuntimeContext(PubSubTopic pubSubTopic) throws Exception {
		Properties consumerProps = createConsumerConfig(pubSubTopic);

		ConsumerRuntimeContext ctx = new ConsumerRuntimeContext();

		KafkaConsumerRunnable consumerThread = null;
		for (int ctr = 1; ctr <= pubSubTopic.getNoofConsumers(); ctr++) {

			consumerThread = new KafkaConsumerRunnable();

			consumerThread.setConsumerProp(consumerProps);
			consumerThread.setPubSubTopic(pubSubTopic);

			consumerThread.setThreadCounter(ctr);

			ctx.addKafkaConsumerThread(consumerThread);
		}
		return ctx;
	}

	public static Consumer<String, String> createConsumer(Properties consumerConfig, PubSubTopic pubSubTopic) {
		LOGGER.debug("Enter");

		Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
		consumer.subscribe(Arrays.asList(pubSubTopic.getTopic()));

		LOGGER.debug("Exit");
		return consumer;
	}
}
