package test.Kafka;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Test_Producer {

	private static int size = 10000;
	private static Producer<String, String> procuder;
	private static String topicName = "test";
	private static String messageText = "message";

	/**
	 * main 方法
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) {
		try {
			long start = System.currentTimeMillis();
			ExecutorService es = Executors.newFixedThreadPool(10);
			final CountDownLatch cdl = new CountDownLatch(size);
			init();
			for (int a = 0; a < size; a++) {
				es.execute(new Runnable() {
					public void run() {
						sendMessage(cdl.getCount());
						cdl.countDown();
					}
				});
			}
			cdl.await();
			es.shutdown();
			long time = System.currentTimeMillis() - start;
			System.out.println("插入" + size + "条JSON，共消耗：" + (double) time / 1000 + " s");
			System.out.println("平均：" + size / ((double) time / 1000) + " 条/秒");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			procuder.close();
		}

	}

	/**
	 * 初始化配置信息
	 */
	private static void init() {

		Properties props = new Properties();
		props.put("bootstrap.servers", "121.36.56.222:9092");
		props.put("acks", "1");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		procuder = new KafkaProducer<String, String>(props);

	}

	/**
	 * 发送消息
	 * @throws InterruptedException
	 */
	private static void sendMessage(long index) {

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, String.valueOf(index), messageText);
		procuder.send(record, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				
			}
		});

	}

}
