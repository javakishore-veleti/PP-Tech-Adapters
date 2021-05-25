package jk.pp.ta.pubsub.kafka.consumer.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jk.pp.ta.pubsub.kafka.consumer.ContinuousConsumerImpl;
import jk.pp.ta.pubsub.kafka.consumer.InermittenConsumerImpl;

@ConditionalOnProperty(name = "pp.ta.pubsub.kafka.consumer.apache.enabled", havingValue = "true")
@Configuration
public class KafkaConsumerGlobalConfig {

//	@Autowired
//	@Qualifier(AppGlobalObjects.EXECUTOR_SVC_BEAN_ID)
//	public ExecutorService consumerExecSvc;

	@ConditionalOnProperty(name = "pp.ta.pubsub.kafka.consumer.apache.enabled", havingValue = "true")
	@Bean(name = "KafkaApacheConsumerContinuous")
	public ContinuousConsumerImpl createContinuousConsumer() {
		// return new ContinuousConsumerImpl(consumerExecSvc);
		return new ContinuousConsumerImpl();
	}

	@ConditionalOnProperty(name = "pp.ta.pubsub.kafka.consumer.apache.enabled", havingValue = "true")
	@Bean(name = "KafkaApacheConsumerIntermittent")
	public InermittenConsumerImpl createIntermittentConsumer() {
		// return new InermittenConsumerImpl(consumerExecSvc);
		return new InermittenConsumerImpl();
	}

}
