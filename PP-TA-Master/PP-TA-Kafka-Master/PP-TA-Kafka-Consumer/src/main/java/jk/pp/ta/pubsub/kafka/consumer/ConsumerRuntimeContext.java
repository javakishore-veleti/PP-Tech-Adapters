package jk.pp.ta.pubsub.kafka.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import jk.pp.engg.foundations.common.core.pubsub.PubSubTopic;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerRuntimeContext {

	private PubSubTopic pubSubTopic;
	private List<KafkaConsumerRunnable> consumers;

	public void addKafkaConsumerThread(KafkaConsumerRunnable thread) {
		System.out.println("ConsumerRuntimeContext::addKafkaConsumerThread Entered");

		if (this.consumers == null) {
			this.consumers = new ArrayList<>();
		}

		this.consumers.add(thread);
	}

	public void initiateConsumerThreads(ExecutorService consumerExecSvc) {
		System.out.println("ConsumerRuntimeContext::initiateConsumerThreads Entered");

		if (this.consumers == null || this.consumers.size() == 0) {
			System.out.println(
					"ConsumerRuntimeContext::initiateConsumerThreads Exiting consumers == null || this.consumers.size() == 0");
			return;
		}

		consumers.forEach(aConsumer -> {

			Thread aConsumerThread = new Thread(aConsumer);
			aConsumerThread.start();

			aConsumer.setThreadStarted(Boolean.TRUE);
		});

		System.out.println("ConsumerRuntimeContext::initiateConsumerThreads Exiting");
	}

}
