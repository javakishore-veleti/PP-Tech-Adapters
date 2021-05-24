package jk.pp.ta.pubsub.kafka.producer;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import jk.pp.engg.foundations.common.core.pubsub.PubSubKey;
import jk.pp.engg.foundations.common.core.pubsub.PubSubMessage;
import jk.pp.engg.foundations.common.core.pubsub.PubSubResult;
import jk.pp.engg.foundations.common.pubsub.AppPubSubPublisher;
import jk.pp.engg.foundations.common.pubsub.PubSubCallBackHandler;

@ConditionalOnProperty(name = "pp.ta.pubsub.kafka.producer.apache.enabled", havingValue = "true")
@Component("KafkaProducerContinuousImpl")
public class KafkaProducerContinuousImpl implements AppPubSubPublisher<PubSubKey, PubSubMessage, PubSubResult> {

	@Override
	public PubSubResult publish(String topicName, PubSubKey key, PubSubMessage message,
			PubSubCallBackHandler<PubSubKey, PubSubMessage, PubSubResult> callBackHandler) throws Exception {

		PubSubResult result = new PubSubResult();
		if (callBackHandler != null) {
			callBackHandler.callback(key, message, result);
		}
		return new PubSubResult();
	}

}
