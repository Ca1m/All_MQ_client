package test.ActiveMQ;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * 从 MQ 取消息，处理之后回送 新消息到 指定的队列
 * 
 * @author Infosec_jy
 */
public class Test_Consumer {

	private static final String BROKEN_URL = "tcp://10.20.86.101:61616";
	private static Session session_c;
	private static Connection connection;
	private static MessageConsumer consumer;

	public static void main(String[] args) throws Exception {

		init();
		getMessage(); // 消费者获取消息,处理之后，回送消息到 broker
	}

	/**
	 * 初始化连接
	 * 
	 * @throws JMSException
	 */
	public static void init() throws JMSException {

		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKEN_URL);
		connection = connectionFactory.createConnection();
		connection.start();
		session_c = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		Queue queue = session_c.createQueue("test_0701");
		consumer = session_c.createConsumer(queue);

	}

	/**
	 * 获取 MQ 的信息
	 * @param disname
	 * @throws InvalidProtocolBufferException
	 * @throws JMSException
	 */
	public static void getMessage() throws Exception {
		try {
			while (true) {
				BytesMessage msg = (BytesMessage) consumer.receive();
				// ActiveMQTextMessage msg = (ActiveMQTextMessage) consumer.receive();
				if (msg != null) {

					msg.acknowledge();

				}
			}
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
}





