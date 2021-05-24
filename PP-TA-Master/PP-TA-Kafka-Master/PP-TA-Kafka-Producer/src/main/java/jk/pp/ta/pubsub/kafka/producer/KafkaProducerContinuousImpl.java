package jk.pp.ta.pubsub.kafka.producer;

import java.util.List;

import org.javatuples.Pair;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import jk.pp.engg.foundations.common.core.pubsub.PubSubCallBackHandler;
import jk.pp.engg.foundations.common.core.pubsub.PubSubKey;
import jk.pp.engg.foundations.common.core.pubsub.PubSubMessage;
import jk.pp.engg.foundations.common.core.pubsub.PubSubResult;
import jk.pp.engg.foundations.common.core.pubsub.PubSubTopic;
import jk.pp.engg.foundations.common.service.core.pubsub.PubSubProducerService;

@ConditionalOnProperty(name = "pp.ta.pubsub.kafka.producer.apache.enabled", havingValue = "true")
@Component("KafkaProducerContinuousImpl")
public class KafkaProducerContinuousImpl implements PubSubProducerService<PubSubKey, PubSubMessage, PubSubResult> {

	@Override
	public PubSubResult publishMessage(String topic, Pair<PubSubKey, PubSubMessage> msgKeyAndVal,
			PubSubTopic pubSubTopic, PubSubCallBackHandler<PubSubKey, PubSubMessage, PubSubResult> callBackHandler) throws Exception {

		PubSubResult result = new PubSubResult();
		if (callBackHandler != null) {
			callBackHandler.callback(msgKeyAndVal, result);
		}

		return result;
	}

	@Override
	public PubSubResult publishMessages(String topic, List<Pair<PubSubKey, PubSubMessage>> msgKeyAndVals,
			PubSubTopic pubSubTopic, PubSubCallBackHandler<PubSubKey, PubSubMessage, PubSubResult> callBackHandler) throws Exception {

		PubSubResult result = new PubSubResult();
		if (callBackHandler != null) {
			callBackHandler.callback(msgKeyAndVals, result);
		}

		return result;
	}

}
