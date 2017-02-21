package kafkaProducer;

//import org.apache.kafka.clients.javaapi.producer.Producer; 
import org.apache.kafka.clients.producer.ProducerRecord; 
//import org.apache.kafka.clients.producer.ProducerConfig; 
import org.apache.kafka.clients.producer.KafkaProducer;
import org.codehaus.jettison.json.JSONException; 
import org.codehaus.jettison.json.JSONObject; 
//import org.slf4j.Logger; 
//import org.slf4j.LoggerFactory; 


import com.google.common.io.Resources; 

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Properties; 
import java.util.Random;


public class KafkaJsonProducer {

	 
 /*
	    // Logger 
//	    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonProducer.class); 
	private static int getRandomNumberInRange(int min, int max) {

		if (min >= max) {
			throw new IllegalArgumentException("max must be greater than min");
		}

		Random r = new Random();
		return r.nextInt((max - min) + 1) + min;
	}
*/
	   public static void main(String[] args) throws IOException {
	        // Properties props = new Properties(); 
	        // props.put("metadata.broker.list", getKafkaHostname() + ":" + getKafkaPort());
	        // props.put("metadata.broker.list", "kafka" + ":" + "9092");
	        // props.put("serializer.class", "kafka.serializer.StringEncoder"); 
	        
	        //ProducerConfig config = new ProducerConfig(props); 
	     // set up the producer
	        KafkaProducer<String, String> producer;
	        try (InputStream props = Resources.getResource("producer.props").openStream()) {
	            Properties properties = new Properties();
	            properties.load(props);
	            producer = new KafkaProducer<>(properties);
	        }
	 
	        // Send 10 messages to the local kafka server: 
//	        LOG.info("KAFKA: Preparing to send {} initial messages", messageCount); 
	        String[] custNames = {"Alex", "John", "Mark", "Dan", "Jim", "Mike", "Steve", "Pam", "Mary", "Jill"};
	        String[] merchants = {"Amazon","Gap", "Walmart", "Google", "Expedia", "Kayak", "Target", "Kohls", "WholeFoods", "Kroger" };
	        for (int i=1; i<=10; i++){ 
	        	java.util.Date date= new java.util.Date();
	            // Create the JSON object 
	            JSONObject obj = new JSONObject(); 
	            try { 
	                obj.put("acct_num", "a100" + String.valueOf(i)); 
	                //obj.put("tran_amt", String.valueOf(getRandomNumberInRange(101,999)));
	                obj.put("tran_amt", "200" + String.valueOf(i));
	                obj.put("tran_date", new Timestamp(date.getTime()).toString());
	                //obj.put("cust_name", custNames[getRandomNumberInRange(0,9)] );
	                //obj.put("merchant", merchants[getRandomNumberInRange(0,9)] );
	                obj.put("cust_name", custNames[i-1] );
	                obj.put("merchant", merchants[i-1] );
	                 
	            } catch(JSONException e) { 
	                e.printStackTrace(); 
	            } 
	            String payload = obj.toString(); 
	            System.out.println("Payload=" + payload);
	         //   KeyedMessage<String, String> data1 = new KeyedMessage<String, String>(getTopic(), null, payload); 
	            ProducerRecord<String, String> data = new ProducerRecord<String, String>("transaction",null,payload);
	            producer.send(data); 
	           
	        } 
	        
	         
	        producer.close(); 
	    } 
	     
	}
