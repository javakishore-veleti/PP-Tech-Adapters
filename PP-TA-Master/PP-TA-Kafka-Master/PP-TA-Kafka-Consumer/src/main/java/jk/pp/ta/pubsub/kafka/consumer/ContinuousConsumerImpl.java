package jk.pp.ta.pubsub.kafka.consumer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import jk.pp.engg.foundations.common.core.pubsub.PubSubKey;
import jk.pp.engg.foundations.common.core.pubsub.PubSubMessage;
import jk.pp.engg.foundations.common.core.pubsub.PubSubResult;
import jk.pp.engg.foundations.common.core.pubsub.PubSubTopic;
import jk.pp.engg.foundations.common.service.core.pubsub.PubSubConsumerService;

public class ContinuousConsumerImpl extends KafkaBaseConsumerImpl<PubSubKey, PubSubMessage, PubSubResult>
		implements PubSubConsumerService<PubSubKey, PubSubMessage, PubSubResult> {

	// private ExecutorService executorSvc = null;
	private ConcurrentHashMap<String, ConsumerRuntimeContext> consumers = new ConcurrentHashMap<String, ConsumerRuntimeContext>();

	public ContinuousConsumerImpl() {
	}

	public ContinuousConsumerImpl(ExecutorService executorSvc) {
		// this.setExecutorService(executorSvc);
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

		System.out.println("consumeMessage topic -> " + pubSubTopic);

		ConsumerRuntimeContext consumerCtx = this.getOrCreateConsumer(pubSubTopic);
		// consumerCtx.initiateConsumerThreads(this.executorSvc);
		consumerCtx.initiateConsumerThreads(null);

		return new PubSubResult();
	}

}
