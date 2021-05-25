package jk.pp.ta.pubsub.kafka.consumer;

import jk.pp.engg.foundations.common.core.pubsub.PubSubKey;
import jk.pp.engg.foundations.common.core.pubsub.PubSubMessage;
import jk.pp.engg.foundations.common.core.pubsub.PubSubResult;
import jk.pp.engg.foundations.common.core.pubsub.PubSubTopic;
import jk.pp.ta.pubsub.kafka.consumer.util.KafkaConnectUtil;

// https://github.com/howtoprogram/Kafka-MultiThread-Java-Example/blob/555e2a7ac38dd32f9200b50a55531ea6eae5f1be/src/main/java/com/howtoprogram/kafka/multipleconsumers/NotificationConsumerThread.java#L10
public class KafkaBaseConsumerImpl<K extends PubSubKey, V extends PubSubMessage, R extends PubSubResult> {

	protected ConsumerRuntimeContext createConsumerRuntimeContext(PubSubTopic pubSubTopic) throws Exception {

		System.out.println("consumerMessageFromKafka pubSubTopic -> " + pubSubTopic);
		return KafkaConnectUtil.createConsumerRuntimeContext(pubSubTopic);
	}

}
