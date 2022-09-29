package cs523.TweetsStreamer;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TweetsStreamer {
	Logger logger = LoggerFactory.getLogger(TweetsStreamer.class.getName());
	public static final String BOOTSTRAPSERVERS  = "127.0.0.1:9092";
	public static final String TOPIC = "SaTweetsTopic";
	public static final String ACKS_CONFIG = "all";
	public static final String MAX_IN_FLIGHT_CONN = "5";
	public static final String COMPRESSION_TYPE = "snappy";
	public static final String RETRIES_CONFIG = Integer.toString(Integer.MAX_VALUE);
	public static final String LINGER_CONFIG = "20";
	public static final String BATCH_SIZE = Integer.toString(32*1024);
	public static final String TWITTER_API_KEY = "d6zQVJOYDGSTuHN4kiM0ORP7X";
	public static final String TWITTER_API_SECRET = "vIelxZdrnmfD7Qvb0L4NygoyD3pdbinv1ohmvxI9SWsjBPdOKO";
	public static final String TWITTER_TOKEN = "953488010997362688-5Z9WlCtuDw6kcJWUy8B8BJCTbeMPXsZ";
	public static final String TWITTER_SECRET = "aic4omVl0ALy6d6rgtyKIsOe3zXvCjGiMWCzhuSFeBMeM";
	
	public TweetsStreamer() {}

	public static void main(String[] args) {
		new TweetsStreamer().run();
	}

	public void run() {
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		Client client = createTweetClient(msgQueue);
		client.connect();
		KafkaProducer<String, String> producer = createKafkaProducer();

		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if (msg != null) {
				logger.info(msg);
				producer.send(new ProducerRecord<>("SaTweetsTopic", null, msg),
					new Callback() {
						@Override
						public void onCompletion(RecordMetadata recordMetadata, Exception e) {
							if (e != null) {
								logger.error("err: ", e);
							}
						}
					});
			}
		}
	}

	public Client createTweetClient(BlockingQueue<String> msgQueue) {
		
		
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		List<String> terms = Lists.newArrayList("covid");
		hosebirdEndpoint.trackTerms(terms);
		Authentication hosebirdAuth = new OAuth1(TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_TOKEN, TWITTER_SECRET);
		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client")
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}
	
	public KafkaProducer<String, String> createKafkaProducer() {
		Properties prop = new Properties();
	    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAPSERVERS);
	    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	    prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
	    prop.setProperty(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG);
	    prop.setProperty(ProducerConfig.RETRIES_CONFIG, RETRIES_CONFIG);
	    prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT_CONN);
	    prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
	    prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, LINGER_CONFIG);
	    prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);

	    return new KafkaProducer<String, String>(prop);

	}
}