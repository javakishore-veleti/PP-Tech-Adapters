package jk.pp.ta.pubsub.kafka.consumer;

import java.util.concurrent.ExecutorService;

public class InermittenConsumerImpl {

	@SuppressWarnings("unused")
	private ExecutorService executorSvc = null;

	public InermittenConsumerImpl() {
	}

	public InermittenConsumerImpl(ExecutorService executorSvc) {
		this.setExecutorService(executorSvc);
	}

	public void setExecutorService(ExecutorService executorSvc) {
		this.executorSvc = executorSvc;
	}
}
