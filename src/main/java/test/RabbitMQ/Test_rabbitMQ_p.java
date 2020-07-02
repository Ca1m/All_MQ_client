package test.RabbitMQ;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

/***
 * @ClassName: Producer1
 */
public class Test_rabbitMQ_p {

	private static int size = 10000;
	private static final String EXCHANGE = "";
	private static final String QUEUE = "ocsp";
	private static String message = "hello maxchen";
	private static Channel channel;
	private static Connection connection;

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		ExecutorService es = Executors.newFixedThreadPool(10);
		final CountDownLatch cdl = new CountDownLatch(size);
		init_connection();
		for (int a = 0; a < size; a++) {
			es.execute(new Runnable() {
				public void run() {
					try {
						sendMessage();
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
		System.out.println("插入" + size + "条JSON，共消耗：" + (double) time / 1000 + " s");
		System.out.println("平均：" + size / ((double) time / 1000) + " 条/秒");
		channel.close();
		connection.close();
	}

	/**
	 * 初始化 连接
	 * @throws Exception
	 */
	public static void init_connection() throws Exception {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost("10.20.61.158");
		connectionFactory.setPort(5673);
		connectionFactory.setVirtualHost("/");
		connection = connectionFactory.newConnection();
		
		channel = connection.createChannel();
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("x-ha-policy", "all");
		//channel.exchangeDeclare(EXCHANGE, "direct", true);     // 交换器持久化
		channel.queueDeclare(QUEUE, true, false, false, map); //  queue 持久化
		channel.confirmSelect();

		// 添加一个确认监听
		channel.addConfirmListener(new ConfirmListener() {
			public void handleAck(long deliveryTag, boolean multiple) {
				
			}
			public void handleNack(long deliveryTag, boolean multiple) {
				System.err.println(deliveryTag);
				System.err.println("-------no ack!-----------");
			}
		});
	}
	/**
	 * 发送持久化消息 通过 持久化交换器到 持久化队列
	 * @throws Exception
	 */
	public static void sendMessage() throws Exception {
		channel.basicPublish(EXCHANGE, QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes()); // 消息持久化
	}

	public static void addConfirmListener(Channel channel) {

	}

}
