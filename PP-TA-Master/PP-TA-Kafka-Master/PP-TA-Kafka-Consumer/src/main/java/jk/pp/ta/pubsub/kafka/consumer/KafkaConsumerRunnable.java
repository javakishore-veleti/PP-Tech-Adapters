package jk.pp.ta.pubsub.kafka.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import jk.pp.engg.foundations.common.core.pubsub.PubSubKey;
import jk.pp.engg.foundations.common.core.pubsub.PubSubMessage;
import jk.pp.engg.foundations.common.core.pubsub.PubSubTopic;
import jk.pp.ta.pubsub.kafka.consumer.util.KafkaConnectUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaConsumerRunnable implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerRunnable.class);

	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private Boolean keepPoolingKafka = Boolean.TRUE;
	private Boolean threadStarted = Boolean.FALSE;

	private Integer threadCounter = 0;
	private Integer consumerStartDelay = 15;

	private PubSubTopic pubSubTopic;
	private Properties consumerProp;

	private final ReentrantLock startStopLock = new ReentrantLock();
	private volatile boolean stopped = false;
	private volatile boolean started = false;
	private volatile boolean finished = false;
	@SuppressWarnings("rawtypes")
	private final CompletableFuture completion = new CompletableFuture<>();

	@Override
	public void run() {

		startStopLock.lock();
		if (stopped) {
			return;
		}

		started = true;
		startStopLock.unlock();

		try {

			LOGGER.debug("Sleeping [consumerStartDelay] for " + consumerStartDelay);
			Thread.sleep(consumerStartDelay);

		} catch (InterruptedException e) {
			LOGGER.error("Exception occured", e);
		}

		Consumer<String, String> consumer = null;
		try {
			consumer = KafkaConnectUtil.createConsumer(consumerProp, pubSubTopic);

			LOGGER.debug("this.keepPoolingKafka " + this.keepPoolingKafka);

			while (this.keepPoolingKafka) {

				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));

				List<Pair<PubSubKey, PubSubMessage>> recordsList = new ArrayList<>();

				records.forEach(aRecord -> {
					try {

						PubSubKey key = OBJECT_MAPPER.readValue(aRecord.value(), PubSubKey.class);
						PubSubMessage msg = OBJECT_MAPPER.readValue(aRecord.value(), PubSubMessage.class);

						Pair<PubSubKey, PubSubMessage> aRecordPair = new Pair<PubSubKey, PubSubMessage>(key, msg);
						recordsList.add(aRecordPair);

					} catch (IOException e) {
						e.printStackTrace();
					}
				});
			}
		} catch (Exception e) {
			LOGGER.error("Exception occured", e);
		} finally {
			if (consumer != null) {

				LOGGER.info("Finally block, closing the Kafka Consumer Connection");
				consumer.close();
			}
		}

	}

	@SuppressWarnings("unchecked")
	public void stop() {
		startStopLock.lock();
		this.stopped = true;
		if (!started) {
			finished = true;
			completion.complete(-1L);
		}
		startStopLock.unlock();
	}

	public long waitForCompletion() {
		try {
			return (long) completion.get();
		} catch (InterruptedException | ExecutionException e) {
			return -1;
		}
	}

	public boolean isFinished() {
		return finished;
	}

}
