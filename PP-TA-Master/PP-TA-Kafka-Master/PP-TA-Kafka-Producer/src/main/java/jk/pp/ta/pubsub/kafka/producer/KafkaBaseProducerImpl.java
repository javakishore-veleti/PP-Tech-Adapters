package jk.pp.ta.pubsub.kafka.producer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.javatuples.Pair;

import jk.pp.engg.foundations.common.core.pubsub.PubSubKey;
import jk.pp.engg.foundations.common.core.pubsub.PubSubMessage;
import jk.pp.engg.foundations.common.core.pubsub.PubSubResult;
import jk.pp.engg.foundations.common.core.pubsub.PubSubTopic;

public class KafkaBaseProducerImpl<K extends PubSubKey, V extends PubSubMessage, R extends PubSubResult> {

	protected Properties createProducerConfig(PubSubTopic pubSubTopic) throws Exception {

		Properties props = new Properties();

		InputStream iStream = null;
		String fileName = pubSubTopic.getAdapterProducerConfig();

		try {
			iStream = this.getClass().getClassLoader().getResourceAsStream(fileName);

			if (iStream == null) {
				throw new Exception("Kafka Producer configuration file does not exists -> " + fileName);
			}

			props.load(iStream);

		} catch (IOException exception) {
			throw new Exception(exception);
		} finally {
			if (iStream != null) {
				iStream.close();
			}
		}

		return props;
	}

	protected Producer<String, String> createProducer(PubSubTopic pubSubTopic) throws Exception {
		Properties producerProps = this.createProducerConfig(pubSubTopic);
		Producer<String, String> producer = new KafkaProducer<>(producerProps);
		return producer;
	}

	protected PubSubResult publishMessagesToKafka(List<Pair<K, V>> msgKeyAndVals, Producer<String, String> producer,
			PubSubTopic pubSubTopic) {

		msgKeyAndVals.forEach(aMessage -> {
			this.publishAMessageToKafka(aMessage, producer, pubSubTopic);
		});

		return new PubSubResult();
	}

	protected PubSubResult publishAMessageToKafka(Pair<K, V> msgKeyAndVal, Producer<String, String> producer,
			PubSubTopic pubSubTopic) {

		System.out.println("publishAMessageToKafka msgKeyAndVal -> " + msgKeyAndVal);

		producer.send(new ProducerRecord<String, String>(pubSubTopic.getTopic(),
				msgKeyAndVal.getValue0().generateKeyString(), msgKeyAndVal.getValue1().generateMessageAsJson()));

		return new PubSubResult();
	}

}
