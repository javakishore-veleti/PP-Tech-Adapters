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
	private final CompletableFuture completion = new CompletableFuture<>();

	@Override
	public void run() {

		startStopLock.lock();
		if (stopped) {
			return;
		}

		started = true;
		startStopLock.unlock();

		System.out.println("KafkaConsumerRunnable run");

		try {
			Thread.sleep(consumerStartDelay);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Consumer<String, String> consumer = null;
		try {
			consumer = KafkaConnectUtil.createConsumer(consumerProp, pubSubTopic);

			System.out.println("KafkaConsumerRunnable this.keepPoolingKafka " + this.keepPoolingKafka);

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
			e.printStackTrace();
		} finally {
			if (consumer != null) {
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
