package jk.pp.ta.pubsub.kafka.producer;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.producer.Producer;
import org.javatuples.Pair;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import jk.pp.engg.foundations.common.core.pubsub.PubSubCallBackHandler;
import jk.pp.engg.foundations.common.core.pubsub.PubSubKey;
import jk.pp.engg.foundations.common.core.pubsub.PubSubMessage;
import jk.pp.engg.foundations.common.core.pubsub.PubSubResult;
import jk.pp.engg.foundations.common.core.pubsub.PubSubTopic;
import jk.pp.engg.foundations.common.service.core.pubsub.PubSubProducerService;

@ConditionalOnProperty(name = "pp.ta.pubsub.kafka.producer.apache.enabled", havingValue = "true")
public class ContinuousProducerImpl extends KafkaBaseProducerImpl<PubSubKey, PubSubMessage, PubSubResult>
		implements PubSubProducerService<PubSubKey, PubSubMessage, PubSubResult> {

	private ConcurrentHashMap<String, Producer<String, String>> producers = new ConcurrentHashMap<String, Producer<String, String>>();

	private Producer<String, String> getOrCreateProducer(PubSubTopic pubSubTopic) throws Exception {

		Producer<String, String> producer = this.producers.get(pubSubTopic.getTopic());
		if (producer == null) {
			producer = this.createProducer(pubSubTopic);
			this.producers.put(pubSubTopic.getTopic(), producer);
		}

		return producer;
	}

	@Override
	public PubSubResult publishMessage(String topic, Pair<PubSubKey, PubSubMessage> msgKeyAndVal,
			PubSubTopic pubSubTopic, PubSubCallBackHandler<PubSubKey, PubSubMessage, PubSubResult> callBackHandler)
			throws Exception {

		System.out.println("publishMessage topic -> " + topic);

		PubSubResult result = this.publishAMessageToKafka(msgKeyAndVal, this.getOrCreateProducer(pubSubTopic),
				pubSubTopic);

		if (callBackHandler != null) {
			callBackHandler.callback(msgKeyAndVal, result);
		}

		return result;
	}

	@Override
	public PubSubResult publishMessages(String topic, List<Pair<PubSubKey, PubSubMessage>> msgKeyAndVals,
			PubSubTopic pubSubTopic, PubSubCallBackHandler<PubSubKey, PubSubMessage, PubSubResult> callBackHandler)
			throws Exception {

		PubSubResult result = this.publishMessagesToKafka(msgKeyAndVals, this.getOrCreateProducer(pubSubTopic),
				pubSubTopic);

		if (callBackHandler != null) {
			callBackHandler.callback(msgKeyAndVals, result);
		}

		return result;
	}

}
