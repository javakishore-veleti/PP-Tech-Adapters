package jk.pp.ta.pubsub.kafka.producer;

import java.util.List;

import org.apache.kafka.clients.producer.Producer;
import org.javatuples.Pair;

import jk.pp.engg.foundations.common.core.pubsub.PubSubCallBackHandler;
import jk.pp.engg.foundations.common.core.pubsub.PubSubKey;
import jk.pp.engg.foundations.common.core.pubsub.PubSubMessage;
import jk.pp.engg.foundations.common.core.pubsub.PubSubResult;
import jk.pp.engg.foundations.common.core.pubsub.PubSubTopic;
import jk.pp.engg.foundations.common.service.core.pubsub.PubSubProducerService;

public class IntermittentProducerImpl extends KafkaBaseProducerImpl<PubSubKey, PubSubMessage, PubSubResult>
		implements PubSubProducerService<PubSubKey, PubSubMessage, PubSubResult> {

	@Override
	public PubSubResult publishMessage(String topic, Pair<PubSubKey, PubSubMessage> msgKeyAndVal,
			PubSubTopic pubSubTopic, PubSubCallBackHandler<PubSubKey, PubSubMessage, PubSubResult> callBackHandler)
			throws Exception {

		Producer<String, String> producer = this.createProducer(pubSubTopic);

		PubSubResult result = this.publishAMessageToKafka(msgKeyAndVal, producer, pubSubTopic);

		if (callBackHandler != null) {
			callBackHandler.callback(msgKeyAndVal, result);
		}

		producer.close();

		return result;
	}

	@Override
	public PubSubResult publishMessages(String topic, List<Pair<PubSubKey, PubSubMessage>> msgKeyAndVals,
			PubSubTopic pubSubTopic, PubSubCallBackHandler<PubSubKey, PubSubMessage, PubSubResult> callBackHandler)
			throws Exception {

		Producer<String, String> producer = this.createProducer(pubSubTopic);

		PubSubResult result = this.publishMessagesToKafka(msgKeyAndVals, producer, pubSubTopic);

		if (callBackHandler != null) {
			callBackHandler.callback(msgKeyAndVals, result);
		}

		producer.close();

		return result;
	}

}
