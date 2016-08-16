package c2w2;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
public class consumer {
    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "topic2";
    private static final String TOPIC3 = "topic3";
    private static final String TOPIC4 = "topic4";

    public static int result1=0;
    public static int result2=0;
    public static int result3=0;
    public static int result4=0;
    private static final int NUM_THREADS = 1;
    
    public static void fuction () throws Exception
    {
    	Properties props = new Properties();
       	props.put("group.id", "test-group");
       	props.put("zookeeper.connect", "kafka1:2181,kafka2:2181,kafka3:2181");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        
    	topicCountMap.put(TOPIC4, NUM_THREADS);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(TOPIC4);
    	ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    	for (final KafkaStream<byte[], byte[]> stream : streams) {
    		executor.execute(new Runnable() {
            
    			public synchronized void run() {
    				for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
    					String tmp = new String(messageAndMetadata.message());
    					
    			
    					
    					result4 += Integer.parseInt(tmp);
    				
    					
    				}
    				}
    		
    			
    			});
    		
    			
    	}
    	
    	
    	Thread.sleep(6000);
        
    	
    	System.out.println("result : "+result4);
    	
    	consumer.shutdown();
    	executor.shutdown();
    }
    public static void main(String[] args) throws Exception {
      
    	
    	Properties props = new Properties();
       	props.put("group.id", "test-group");
       	props.put("zookeeper.connect", "kafka1:2181,kafka2:2181,kafka3:2181");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        
        Properties prop = new Properties();
        prop.put("metadata.broker.list", "kafka1:9092,kafkat2:9092,kafka3:9092");
        prop.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig  producerConfig = new ProducerConfig(prop);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);
             
        
        
        
    	if(args[0].equals("1"))
        { 
    		topicCountMap.put(TOPIC1, NUM_THREADS);
    		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

    		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(TOPIC1);
        	ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        	for (final KafkaStream<byte[], byte[]> stream : streams) {
        		executor.execute(new Runnable() {
                
        			public synchronized void run() {
        				for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
        					String tmp = new String(messageAndMetadata.message());
        					
        			
        					
        					result1 += Integer.parseInt(tmp);
        				
        					
        				}
        				}
        		
        			
        			});
        		
        			
        	}
        	
        	
        	Thread.sleep(6000);
        
        	System.out.println("result1 : "+result1);
        	
        
        	
        	KeyedMessage<String, String> message = new KeyedMessage<String, String>("topic4", String.valueOf(result1));  
    		producer.send(message);
    		
    			Thread.sleep(1000);
        	fuction();
    		
        	consumer.shutdown();
        	executor.shutdown();
      
        }else if(args[0].equals("2"))
        {
        	topicCountMap.put(TOPIC2, NUM_THREADS);
    		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        	List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(TOPIC2);
        	ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        	for (final KafkaStream<byte[], byte[]> stream : streams) {
        		executor.execute(new Runnable() {
                
        			public synchronized void run() {
        				for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
        					String tmp = new String(messageAndMetadata.message());
        					
        					result2 += Integer.parseInt(tmp);
        				}
        			}
        		});
        	}
        	Thread.sleep(6000);
          	 System.out.println("result2 : "+result2);
          	 
         	KeyedMessage<String, String> message = new KeyedMessage<String, String>("topic4", String.valueOf(result2));  
    		producer.send(message);
    		
        	consumer.shutdown();
        	executor.shutdown();
       
        	
        }else if(args[0].equals("3"))
        {
        	topicCountMap.put(TOPIC3, NUM_THREADS);
    		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        	List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(TOPIC3);
        	ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        	for (final KafkaStream<byte[], byte[]> stream : streams) {
        		executor.execute(new Runnable() {
                
        			public synchronized void run() {
        				for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
        					String tmp = new String(messageAndMetadata.message());
        					
        					result3 += Integer.parseInt(tmp);

        				}
        			}
        		});
        	}
          
        	Thread.sleep(6000);
          	 System.out.println("result3 : "+result3);
         	KeyedMessage<String, String> message = new KeyedMessage<String, String>("topic4", String.valueOf(result3));  
    		producer.send(message);
        	consumer.shutdown();
        	executor.shutdown();
       
        }else if(args[0].equals("4"))
        { 
    	
      
        }
    	
    
}
   
}