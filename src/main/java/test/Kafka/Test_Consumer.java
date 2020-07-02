package test.Kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 从 MQ 取消息，处理之后回送 新消息到 指定的队列
 * 
 * @author Infosec_jy
 */
public class Test_Consumer {

	private static KafkaConsumer<String,String> consumer;

	/**
	*  初始化消费者
	*/
    static {
        Properties configs = initConfig();
        consumer = new KafkaConsumer<String, String>(configs);
        consumer.subscribe(Arrays.asList("Hello-Kafka"));
    }
	/**
	*  初始化配置
	*/
    private static Properties initConfig(){

        Properties props = new Properties();
        
		props.put("bootstrap.servers", "10.20.83.245:9092");
        props.put("group.id", "1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("max.poll.records", "10");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        
        return props;
    }
    
    public static void main(String[] args) {

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach((ConsumerRecord<String, String> record)->{});
        }
    }

}



