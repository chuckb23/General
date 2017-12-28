package flop.twitter.spark;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.logging.LogManager;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import flop.twitter.data.DailyMentions;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 *
 * broker1-host:port,broker2-host:port topic1,topic2 Things to add: 
 * 1. Filtering
 * out symbols we don't care to track (maintain a symbols to track list which we
 * can either store in a db; 
 * 2. Apply weights to mentions based on how many
 * other currencies are mentioned; 
 * 3. Determine when something is not a stock or
 * currency but actually price (a tweet that is $BTC over $16,000 should be
 * weighted solo but under current impl. would not be); 
 * 4. Actually should look to match $whatever to a list of securities to track
 * 5. Replace characters like "," with a space;
 */
public final class SparkTwitterMediaConsumer {
	private static final Pattern SPACE = Pattern.compile(" ");
	private static final String topic = "twitter-test";
	private static final AmazonDynamoDB dbClient = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
	public static void main(String[] args) throws InterruptedException {
		//Config this out 
		String brokers = "localhost:9092";// args[0];
		String topics = topic;// args[1];
		SparkConf sparkConf = new SparkConf().setAppName("SparkTwitterMediaConsumer");
		sparkConf.setMaster("local[2]");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("group.id", "originalTwitter1");

		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {

			private static final long serialVersionUID = -8695571647592827912L;

			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		/*
		 * Parse JSON to get the full tweet text and created date then filter
		 * out the null tweets (retweets basically) returning DStream of
		 * filtered tweets
		 */
		JavaDStream<Tuple2<String, String>> filteredTweets = lines.map(new ParseJson())
				.filter(new Function<Tuple2<String, String>, Boolean>() {
					private static final long serialVersionUID = -6944335187076561744L;

					@Override
					public Boolean call(Tuple2<String, String> tweet) throws Exception {
						return tweet != null;
					}
				});

		/*
		 * Split full tweet text into word return DStream tuple which will be a
		 * tuple like so (createdDate, List<words in tweet>)
		 */
		JavaDStream<Tuple2<String, List<String>>> words = filteredTweets
				.map(new Function<Tuple2<String, String>, Tuple2<String, List<String>>>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = -5417451409646247498L;

					@Override
					public Tuple2<String, List<String>> call(Tuple2<String, String> x) {
						return new Tuple2<String, List<String>>(x._1(), Lists.newArrayList(SPACE.split(x._2())));
					}
				});
		/*
		 * For each RDD go through the list of words contained in the tweet any
		 * words which start with "$" symbol will then be written to dynamodb
		 * with input of (createdDate, symbol, goodMedia (not implemented),
		 * badMedia (not implemented), totalMedia(1)) Also going to add a
		 * soloMedia field since many tweets are tagged with a bunch of stocks
		 * or cryptocurrencies so they show up when people search. So a tweet
		 * tagged with a solo stock or currency should be worth more than one
		 * tagged with say 10 others (may make since to make this weighted at
		 * some point eg. 1/10 symbols are $BTC thus get .1 score for mention)
		 */
		words.foreachRDD(new VoidFunction<JavaRDD<Tuple2<String, List<String>>>>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<Tuple2<String, List<String>>> tweet) {
				tweet.foreach(new VoidFunction<Tuple2<String, List<String>>>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = -4405700236163580160L;

					@Override
					public void call(Tuple2<String, List<String>> t) {
						t._2().forEach(k -> {
							if (!(k.length() == 0) && k.charAt(0) == '$') {
								try {
									/*
									 * This may want to be changed up. In stead of passing to read write
									 * and just have that handle everything may want to consider returning 
									 * a DailyMention from readWrite adding that to the list and doing a 
									 * batch write. 
									 */
									readWrite(k, t._1(), 0.0d);
								} catch (Exception e) {
									/*Coudnt write new record*/
									e.printStackTrace();
								}
							}
						});
					}
				});
			}

		});
		words.print();

		jssc.start();
		jssc.awaitTermination();
	}

	public static class ParseJson implements Function<String, Tuple2<String, String>> {
		private static final long serialVersionUID = 42l;
		private final ObjectMapper mapper = new ObjectMapper();

		@Override
		public Tuple2<String, String> call(String tweet) {
			try {
				JsonNode root = mapper.readValue(tweet, JsonNode.class);
				if (root.get("extended_tweet") != null && root.get("extended_tweet").get("full_text") != null) {
					String createDate = root.get("created_at").asText();
					String text = root.get("extended_tweet").get("full_text").asText();
					return new Tuple2<String, String>(createDate, text);
				}
				return null;
			} catch (IOException ex) {
				Logger LOG = Logger.getLogger(this.getClass());
				LOG.error("IO error while filtering tweets", ex);
				LOG.trace(null, ex);
			}
			return null;
		}
	}

	

	private static void readWrite(String symbol, String date, double sentimentScore)
			throws ClassNotFoundException, ParseException {
		DynamoDBMapper mapper = new DynamoDBMapper(dbClient);
		String upperCaseSymbol = symbol.toUpperCase();
		String parsedDate = parseDate(date);
		DailyMentions item = mapper.load(DailyMentions.class, upperCaseSymbol, parsedDate);
		if (item == null) {
			item = new DailyMentions(parsedDate, upperCaseSymbol, 1);
			//To be moved into constructor
			item.setGoodMedia(0);
			item.setBadMedia(0);
		} else {
			item.setTotalMedia(item.getTotalMedia() + 1);
			item.setGoodMedia(item.getGoodMedia() + 0);
			item.setBadMedia(item.getBadMedia() + 0);
			//item.setSentimentScore(item.getSentimentScore() + sentimentScore);
		}
		mapper.save(item);
	}
	
	
	private static String parseDate(String date) throws ParseException{
		DateFormat df = new SimpleDateFormat("EEE MMM dd kk:mm:ss z yyyy", Locale.ENGLISH);
		DateFormat ff = new SimpleDateFormat("MM/dd/yyyy", Locale.ENGLISH);
		return ff.format(df.parse(date));
	}

}