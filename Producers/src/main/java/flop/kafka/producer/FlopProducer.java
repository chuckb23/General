package flop.kafka.producer;

import java.util.Properties;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.Logger;

public abstract class FlopProducer implements Runnable {
	protected final String topic;
	protected final static Logger LOG = Logger.getLogger(FlopProducer.class);
	protected final static Producer<String, String> producer = createProducer();
	//Get from config
	protected final static String brokers = "localhost:9092";
	
	FlopProducer(HierarchicalConfiguration<ImmutableNode> configNode){
		this.topic = configNode.getString("topic");
		setConfig(configNode);
	}
	public void run(){
		try {
			start();
		} catch (InterruptedException e) {
			LOG.error(e);
		}
	}
	public abstract void start() throws InterruptedException;
	protected abstract void setConfig(HierarchicalConfiguration<ImmutableNode> configNode);
	private static Producer<String, String> createProducer(){
		Properties properties = new Properties();
		/**
		 * Get all of this abstracted out to a config file so code doesn't need to be changed when we 
		 * want to adjust batch size, acks, memory, etc.
		 */
		properties.put("metadata.broker.list", brokers);
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("client.id", "camus");
		properties.put("bootstrap.servers", brokers);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("buffer.memory", 33554432);
		properties.put("linger.ms", 1);
		properties.put("batch.size", 16384);
		//May want to change to just leader will decide at a later point
		properties.put("acks", "all");

		return new KafkaProducer<String, String>(properties);
	}
}
