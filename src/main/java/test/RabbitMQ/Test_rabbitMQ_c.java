package test.RabbitMQ;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Test_rabbitMQ_c {

	private static int size = 400000;
	private static final String EXCHANGE = "ocsp_exchage";
	private static final String QUEUE = "ocsp";
	private static Channel channel;
	private static Connection connection;
	private static Consumer consumer;

	public static void main(String[] args) {
		try {
			long start = System.currentTimeMillis();
			ExecutorService es = Executors.newFixedThreadPool(10);
			final CountDownLatch cdl = new CountDownLatch(size);
			init_connection();
			for (int a = 0; a < size; a++) {
				es.execute(new Runnable() {
					public void run() {
						try {
							receiver_Message();
						} catch (Exception e) {
							e.printStackTrace();
						}
						cdl.countDown();
					}
				});
			}
			cdl.await();
			es.shutdown();
			long time = System.currentTimeMillis() - start;
			System.out.println("消费" + size + "条JSON，共消耗：" + (double) time / 1000 + " s");
			System.out.println("平均：" + size / ((double) time / 1000) + " 条/秒");
		} catch (Exception e) {
			try {
				channel.close();
				connection.close();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}

	public static void init_connection() throws Exception {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost("10.20.61.158");
		connectionFactory.setPort(5672);
		connectionFactory.setVirtualHost("/");

		connection = connectionFactory.newConnection();

		// channel.exchangeDeclare(EXCHANGE, "direct", true); // 交换机持久化
		channel = connection.createChannel();
		channel.queueDeclare(QUEUE, true, false, false, null); // queue 持久化
		channel.confirmSelect();

		consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				channel.basicAck(envelope.getDeliveryTag(), false);
			}
		};
	}

	/**
	 * 获取消息
	 * 
	 * @throws Exception
	 */
	public static void receiver_Message() throws Exception {

		channel.basicConsume(QUEUE, consumer);

	}

	public static void addConfirmListener(Channel channel) {

	}

}
