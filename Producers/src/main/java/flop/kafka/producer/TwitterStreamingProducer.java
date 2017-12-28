package flop.kafka.producer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterStreamingProducer extends FlopProducer {
	private String consumerSecret;
	private String consumerKey;
	private String secret;
	private String token;
	private Authentication auth;
	private List<String> filterTerms;
	
	public TwitterStreamingProducer(HierarchicalConfiguration<ImmutableNode> configNode){
		super(configNode);
		this.auth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
	}
	
	@Override
	public void start() throws InterruptedException{
		start(auth, topic, filterTerms);
	}
	
	private static void start(Authentication auth, String topic, List<String> wordsToTrack)
			throws InterruptedException {

		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(wordsToTrack);

		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST).endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		client.connect();

		// This should be running at all times so infinite but this may be the wrong approach
		for (;;) {
			ProducerRecord<String, String> message = null;
			try {
				message = new ProducerRecord<String, String>(topic, queue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			LOG.info("Message sent");
			producer.send(message);
		}

	}
	
	protected void setConfig(HierarchicalConfiguration<ImmutableNode> configNode){
		//Future: will throw own exception of missing vital property (or something like that)
		consumerSecret = configNode.getString("consumerSecret");
		consumerKey = configNode.getString("consumerKey");
		secret = configNode.getString("secret");
		token = configNode.getString("token");
		filterTerms = parseFilterTerms(configNode.getString("filterTerms"));
	}
	
	private List<String> parseFilterTerms(String termsString){
		return Arrays.asList(termsString.split(","));
	}

}
