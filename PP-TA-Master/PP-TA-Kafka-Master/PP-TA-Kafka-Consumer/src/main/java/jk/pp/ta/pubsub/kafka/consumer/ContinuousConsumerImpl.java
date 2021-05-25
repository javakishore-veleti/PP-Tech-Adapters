package jk.pp.ta.pubsub.kafka.consumer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jk.pp.engg.foundations.common.core.pubsub.PubSubKey;
import jk.pp.engg.foundations.common.core.pubsub.PubSubMessage;
import jk.pp.engg.foundations.common.core.pubsub.PubSubResult;
import jk.pp.engg.foundations.common.core.pubsub.PubSubTopic;
import jk.pp.engg.foundations.common.service.core.pubsub.PubSubConsumerService;

public class ContinuousConsumerImpl extends KafkaBaseConsumerImpl<PubSubKey, PubSubMessage, PubSubResult>
		implements PubSubConsumerService<PubSubKey, PubSubMessage, PubSubResult> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ContinuousConsumerImpl.class);

	private ConcurrentHashMap<String, ConsumerRuntimeContext> consumers = new ConcurrentHashMap<String, ConsumerRuntimeContext>();

	public ContinuousConsumerImpl() {
	}

	private ConsumerRuntimeContext getOrCreateConsumer(PubSubTopic pubSubTopic) throws Exception {

		ConsumerRuntimeContext consumerCtx = this.consumers.get(pubSubTopic.getTopic());
		if (consumerCtx == null) {
			consumerCtx = this.createConsumerRuntimeContext(pubSubTopic);
			this.consumers.put(pubSubTopic.getTopic(), consumerCtx);
		}

		return consumerCtx;
	}

	public void setExecutorService(ExecutorService executorSvc) {
		// this.executorSvc = executorSvc;
	}

	@Override
	public PubSubResult consumeMessages(PubSubTopic pubSubTopic) throws Exception {
		LOGGER.debug("PubSubTopic Topic -> " + pubSubTopic.getTopic());

		ConsumerRuntimeContext consumerCtx = this.getOrCreateConsumer(pubSubTopic);
		// consumerCtx.initiateConsumerThreads(this.executorSvc);
		consumerCtx.initiateConsumerThreads();

		LOGGER.debug("Exit");
		return new PubSubResult();
	}

}
