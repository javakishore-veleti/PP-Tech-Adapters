package jk.pp.ta.pubsub.kafka.producer.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jk.pp.ta.pubsub.kafka.producer.ContinuousProducerImpl;
import jk.pp.ta.pubsub.kafka.producer.IntermittenProducerImpl;

@ConditionalOnProperty(name = "pp.ta.pubsub.kafka.producer.apache.enabled", havingValue = "true")
@Configuration
public class KafkaProducerGlobalConfig {

	@Bean(name = "KafkaApacheProducerContinuous")
	public ContinuousProducerImpl createContinuousProducer() {
		return new ContinuousProducerImpl();
	}

	@Bean(name = "KafkaApacheProducerIntermittent")
	public IntermittenProducerImpl createIntermittentProducerProducer() {
		return new IntermittenProducerImpl();
	}
}
