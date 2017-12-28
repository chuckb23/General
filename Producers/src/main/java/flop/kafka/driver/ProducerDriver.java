package flop.kafka.driver;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.tree.ImmutableNode;

import flop.kafka.producer.FlopProducer;

public class ProducerDriver {
	public static void main(String[] args) {
		try {
			Parameters params = new Parameters();
			FileBasedConfigurationBuilder<XMLConfiguration> builder = new FileBasedConfigurationBuilder<XMLConfiguration>(
					XMLConfiguration.class).configure(params.xml().setFileName("configs.xml"));
			XMLConfiguration config = builder.getConfiguration();
			List<HierarchicalConfiguration<ImmutableNode>> producersList = config.childConfigurationsAt("Producers");
			List<FlopProducer> producerList = new ArrayList<FlopProducer>();
			/**
			 * Grabs the main class for each topic our producer will write to from our config.
			 * Then uses reflection to instantiate that class and add it to our producerList. 
			 */
			for (HierarchicalConfiguration<ImmutableNode> producer : producersList) {
				try {
					String classToLoad = producer.getString("mainclass");
					Class<?> clazz = Class.forName(classToLoad);
					Constructor<?> constructor = clazz.getConstructor(HierarchicalConfiguration.class);
					FlopProducer instanceProducer = (FlopProducer) constructor.newInstance(producer);
					producerList.add(instanceProducer);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			/* Start up a thread for each of our producers
			*currently these producer threads run forever
			*and don't return anything that will probably change for 
			*certain producers 
			**/ 
			for (FlopProducer producer : producerList) {
				new Thread(producer).start(); 
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
