package cs523.HbaseBilguun;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class DataSentimentAnalyser
{
	private static final String TABLE_NAME = "SentimentsTable";
    private static final String CF_TWEET = "Tweet data";
	private static final String CF_SENTIMENT = "Sentiment data";
    private static final String TWEET = "Tweet";
    private static final String SENTIMENT = "Sentiment";

	public static void main(String... args) throws IOException
	{
		
		JSONParser jsonParser = new JSONParser();
	    try {
	       //Parsing the contents of the JSON file
	    	JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader("/home/cloudera/Downloads/tweets/part-00000"));

	       String tweetId = jsonObject.get("id").toString();
	       String tweet = jsonObject.get("text").toString();

			Configuration config = HBaseConfiguration.create();
	
			try (Connection connection = ConnectionFactory.createConnection(config);
					Admin admin = connection.getAdmin())
			{
				HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
    			table.addFamily(new HColumnDescriptor(CF_TWEET).setCompressionType(Algorithm.NONE));
    			table.addFamily(new HColumnDescriptor(CF_SENTIMENT));
	
	
				if (!admin.tableExists(table.getTableName())) {
					System.out.print("Creating table.... ");
					admin.createTable(table);
				}
	
				System.out.println("Done!");
				
				System.out.println("adding rows.... ");
				Table tweetsTable = connection.getTable(TableName.valueOf(TABLE_NAME));
				
				String putId = String.valueOf(tweetId);
          		Put p = new Put(putId.getBytes());
                p.addColumn(CF_TWEET.getBytes(), TWEET.getBytes(), tweet.getBytes());
                // We can call sentiment analysis in here (but it is out of scope of this course)
                p.addColumn(CF_SENTIMENT.getBytes(), SENTIMENT.getBytes(), "positive".getBytes());
                tweetsTable.put(p);
			}

	    } catch (FileNotFoundException e1) {
	       e1.printStackTrace();
	    } catch (IOException e2) {
	          e2.printStackTrace();
	    } catch (ParseException e3) {
	          e3.printStackTrace();
	    }
	}
}