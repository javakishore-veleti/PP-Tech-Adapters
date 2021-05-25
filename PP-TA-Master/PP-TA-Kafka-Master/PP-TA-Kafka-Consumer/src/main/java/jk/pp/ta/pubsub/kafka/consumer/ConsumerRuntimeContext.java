package jk.pp.ta.pubsub.kafka.consumer;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jk.pp.engg.foundations.common.core.pubsub.PubSubTopic;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerRuntimeContext {

	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRuntimeContext.class);

	private PubSubTopic pubSubTopic;
	private List<KafkaConsumerRunnable> consumers;

	public void addKafkaConsumerThread(KafkaConsumerRunnable thread) {
		LOGGER.debug("Enter");

		if (this.consumers == null) {
			this.consumers = new ArrayList<>();
		}

		this.consumers.add(thread);

		LOGGER.debug("Exit");
	}

	public void initiateConsumerThreads() {
		LOGGER.debug("Enter");

		if (this.consumers == null || this.consumers.size() == 0) {
			LOGGER.debug("Exiting consumers == null || this.consumers.size() == 0");
			return;
		}

		consumers.forEach(aConsumer -> {

			Thread aConsumerThread = new Thread(aConsumer);
			aConsumerThread.start();

			aConsumer.setThreadStarted(Boolean.TRUE);
		});

		LOGGER.debug("Exit");
	}

}
