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
	        //String[] custNames = {"Alex", "John", "Mark", "Dan", "Jim", "Mike", "Steve", "Pam", "Mary", "Jill"};
	        //String[] merchants = {"Amazon","Gap", "Walmart", "Google", "Expedia", "Kayak", "Target", "Kohls", "WholeFoods", "Kroger" };
	        String[] random_ctry_risk = {"1","2","3","1","2","3","1","2","3","1","2","3","1","2","3","1","2","3","1","2"};
	        for (int i=1; i<=20; i++){ 
	        	java.util.Date date= new java.util.Date();
	            // Create the JSON object 
	            JSONObject obj = new JSONObject(); 
	            try { 
	                obj.put("cust_id", "c000" + String.valueOf(i)); 
	                //obj.put("tran_amt", String.valueOf(getRandomNumberInRange(101,999)));
	                obj.put("sys_update_dt",new Timestamp(date.getTime()).toString());
	                obj.put("tran_dt", new Timestamp(date.getTime()).toString());
	                obj.put("tran_amt", "200" + String.valueOf(i));
	                obj.put("ctry_risk_rating", random_ctry_risk[i-1]);
	                obj.put("cash_advance_cross_border_flag","Y");
	                obj.put("cash_advance_tran_type_code","6"+ String.valueOf(i));
	                obj.put("cash_withdrawal_tran_type_code","6"+ String.valueOf(i));
	                obj.put("cash_withdrawal_merchant_code","700"+String.valueOf(i));
	                //obj.put("cust_name", custNames[getRandomNumberInRange(0,9)] );
	                //obj.put("merchant", merchants[getRandomNumberInRange(0,9)] );
	                //obj.put("cust_name", custNames[i-1] );
	                //obj.put("merchant", merchants[i-1] );
	                 
	            } catch(JSONException e) { 
	                e.printStackTrace(); 
	            } 
	            String payload = obj.toString(); 
	          //  System.out.println("Payload=" + payload);
	         //   KeyedMessage<String, String> data1 = new KeyedMessage<String, String>(getTopic(), null, payload); 
	            ProducerRecord<String, String> data = new ProducerRecord<String, String>("transaction",null,payload);
	            producer.send(data); 
	           
	        } 
	        
	         
	        producer.close(); 
	    } 
	     
	}
