package test.ActiveMQ;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageProducer;


public class Test_Producer_ {
	static int size = 10000;
	static Session session;
	static ActiveMQMessageProducer producer;
	static Topic topic;
	static Queue queue;
	static Connection connection;
	private static TextMessage message;
	private static String str = "[{'flag':'1','value':'8854c92e92404b188e63c4031db0eac9','label':'交换机)'}]";

	public static void init_connection() throws Exception {
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
				"tcp://121.36.56.222:61616?jms.useAsyncSend=true");
		connection = factory.createConnection();
		connection.start();
		// session = connection.createSession(Boolean.TRUE, Session.SESSION_TRANSACTED);
		session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);


		queue = session.createQueue("ocspResp_1");
		producer = (ActiveMQMessageProducer) session.createProducer(queue);

		producer.setDeliveryMode(DeliveryMode.PERSISTENT);
	}

	public static void sendMessage(TextMessage message) {
		try {
			message = session.createTextMessage();
			message.setText(str);
			producer.send(message);
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}

	public static void close() throws Exception {
		connection.close();
	}

	public static void main(String[] arg) throws Exception {
		long start = System.currentTimeMillis();
		ExecutorService es = Executors.newFixedThreadPool(10);
		final CountDownLatch cdl = new CountDownLatch(size);
		init_connection();
		for (int a = 0; a < size; a++) {
			es.execute(new Runnable() {
				public void run() {
					sendMessage(message);
					cdl.countDown();
				}
			});
		}
		cdl.await();
		es.shutdown();
		long time = System.currentTimeMillis() - start;
		System.out.println("插入" + size + "条JSON，共消耗：" + (double) time / 1000 + " s");
		System.out.println("平均：" + size / ((double) time / 1000) + " 条/秒");
		close();
	}
}