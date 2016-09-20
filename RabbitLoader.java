package cn.test.utils.loader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.test.utils.SystemConfig;

import com.rabbitmq.client.QueueingConsumer.Delivery;


public class RabbitLoader implements AbstractorLoader<Delivery> {

	private static Logger LOG = LoggerFactory.getLogger(RabbitLoader.class);

	private String key;

	private ConnectionFactory factory = null;
	private Connection connection = null;
	private Channel channel = null;
	private QueueingConsumer consumer = null;

	private boolean async = true;
	private boolean autoAck = true;
	private long blockingTime = 10000L;
	private int preFetch = 1000;

	private Address[] addresses;
	private String queueName;
	private String exchangeName;
	private String routingKey;

	public interface RabbitMQConfigure {
		public static String AUTO_ACK = "auto_ack";
		public static String BLOCKING_TIME = "blocking_time";
		public static String PRE_FETCH = "pre_fetch";
	}

	/**
	 * 
	 * @param key index key use for get config info from file
	 * @param async true if use consumer
	 * @param conf configure set
	 */
	public RabbitLoader(String key, Boolean async, Map<String, Object> conf){
		this.key = key;
		this.async = async;

		if (conf.containsKey( RabbitMQConfigure.AUTO_ACK )) {
			this.autoAck = (Boolean)conf.get( RabbitMQConfigure.AUTO_ACK );
		}

		if (conf.containsKey( RabbitMQConfigure.PRE_FETCH)) {
			this.preFetch = (Integer) conf.get( RabbitMQConfigure.PRE_FETCH );
		}

		if (conf.containsKey( RabbitMQConfigure.BLOCKING_TIME )) {
			this.blockingTime = ( Long) conf.get( RabbitMQConfigure.BLOCKING_TIME );
		}
		initConfig();
	}

	private void initConfig() {
		String hosts = SystemConfig.getProperty(key + ".mq.host");
		covertAddress(hosts);

		queueName = SystemConfig.getProperty(key + ".mq.queue_name");
		exchangeName = SystemConfig.getProperty(key + ".mq.exchange_name", "");
		routingKey = SystemConfig.getProperty(key + ".mq.routing_key", "");

		String userName = SystemConfig.getProperty(key + ".mq.username");
		String passWd = SystemConfig.getProperty(key + ".mq.password");

		factory = new ConnectionFactory();
		factory.setUsername(userName);
		factory.setPassword(passWd);
	}

	public void createChannel()
			throws IOException
	{
		connection = factory.newConnection(addresses);
		channel = connection.createChannel();
		channel.basicQos(preFetch);

		queueName = channel.queueDeclare(queueName, false, false, false, null).getQueue();
		channel.queueBind(queueName, exchangeName, routingKey);

		if (this.async){
			consumer = new QueueingConsumer(channel);
			channel.basicConsume(this.queueName, this.autoAck, consumer);
		}
		LOG.info(String.format("connection rabbit mq host[%s]", connection));
	}

	public GetResponse get() throws IOException {
		return this.channel.basicGet( this.queueName, this.autoAck);
	}

	public Delivery next() throws IOException, InterruptedException {
			return consumer.nextDelivery(0);
	}

	public Delivery bnext() throws IOException, InterruptedException {
			return consumer.nextDelivery(this.blockingTime);
	}
	
	public void doAck(Delivery d) throws IOException {
		channel.basicAck(d.getEnvelope().getDeliveryTag(), false);
	}

	public void close() {
		if (null != channel) {
			try {
				channel.close();
			} catch (IOException e) {
				LOG.error("close channel error.");
			}
		}
		if (null != connection) {
			try {
				connection.close();
			} catch (IOException e) {
				LOG.error("close connection error.");
			}
		}

	}

	private void covertAddress(String hosts) {
		String[] hostArr = hosts.split(",");
		int index = 0;
		addresses = new Address[hostArr.length];
		for (String host : hostArr) {
			String[] address = host.split(":");
			addresses[index] = new Address(address[0], Integer.valueOf(address[1]));
			index++;
		}
	}

	public Connection getConnection() {
		return connection;
	}

	public Channel getChannel() {
		return channel;
	}

	public static void main(String[] args) throws UnsupportedEncodingException {

		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(RabbitMQConfigure.AUTO_ACK, true);
		RabbitLoader loader = new RabbitLoader("test", true, conf);
		try {
			loader.createChannel();
		} catch (Exception e) {
			LOG.error("conncetion error", e);
		}

		Delivery d = null;
		while (true) {
			try {
				d = loader.next();
			}catch (Exception e) {
				LOG.error("consumer error!", e);
			}
			if (d == null) {
				LOG.debug("no message, sleep");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					LOG.error("Interrupted!", e);
				}
				continue;
			} else {
				System.out.println("body=" + new String(d.getBody()));
			}
		}
	}

}
