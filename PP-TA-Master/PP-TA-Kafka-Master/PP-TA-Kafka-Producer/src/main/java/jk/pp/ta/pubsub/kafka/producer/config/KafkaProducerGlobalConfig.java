package jk.pp.ta.pubsub.kafka.producer.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jk.pp.ta.pubsub.kafka.producer.ContinuousProducerImpl;
import jk.pp.ta.pubsub.kafka.producer.IntermittentProducerImpl;

@ConditionalOnProperty(name = "pp.ta.pubsub.kafka.producer.apache.enabled", havingValue = "true")
@Configuration
public class KafkaProducerGlobalConfig {

	@ConditionalOnProperty(name = "pp.ta.pubsub.kafka.producer.apache.enabled", havingValue = "true")
	@Bean(name = "KafkaApacheProducerContinuous")
	public ContinuousProducerImpl createContinuousProducer() {
		return new ContinuousProducerImpl();
	}

	@ConditionalOnProperty(name = "pp.ta.pubsub.kafka.producer.apache.enabled", havingValue = "true")
	@Bean(name = "KafkaApacheProducerIntermittent")
	public IntermittentProducerImpl createIntermittentProducerProducer() {
		return new IntermittentProducerImpl();
	}
}
