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
import kafka.message.MessageAndMetadata;
public class consumer {
    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "topic2";
    private static final String TOPIC3 = "topic3";
    public static long result1=0;
    public static long result2=0;
    public static long result3=0;
    private static final int NUM_THREADS = 20;
    public static void main(String[] args) throws Exception {
      
    	int result =0;
    	Properties props = new Properties();
       	props.put("group.id", "test-group");
       	props.put("zookeeper.connect", "kafka1:2181,kafka2:2181,kafka3:2181");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
             
    	if(args[0].equals("1"))
        { 
    		topicCountMap.put(TOPIC1, NUM_THREADS);
    		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

    		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(TOPIC1);
        	ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        	for (final KafkaStream<byte[], byte[]> stream : streams) {
        		
                
        				for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
        					String tmp = new String(messageAndMetadata.message());
        					
        					System.out.println(Integer.parseInt(tmp)+100);
        					
        					result1 += Integer.parseInt(tmp);
        					
        					
        		
        				}
        		
        			
        		
        			
        	}
        	
        	
        	Thread.sleep(60000);
 
        	consumer.shutdown();
        	executor.shutdown();
        	
        }else if(args[0].equals("2"))
        {
        	topicCountMap.put(TOPIC2, NUM_THREADS);
    		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        	List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(TOPIC2);
        	ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        	for (final KafkaStream<byte[], byte[]> stream : streams) {
        		for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
        					String tmp = new String(messageAndMetadata.message());
        					System.out.println(tmp);
        					
        					result2 += Integer.parseInt(tmp);
        				}
        			
        	}
        	
        	Thread.sleep(60000);
        	Thread.sleep(60000);
        	consumer.shutdown();
        	executor.shutdown();
        	
        	
        }else if(args[0].equals("3"))
        {
        	topicCountMap.put(TOPIC3, NUM_THREADS);
    		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        	List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(TOPIC3);
        	ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        	for (final KafkaStream<byte[], byte[]> stream : streams) {
        			for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
        					String tmp = new String(messageAndMetadata.message());
        					System.out.println(tmp);
        					
        					result3 += Integer.parseInt(tmp);

        				
        		}
        	}
        	
        	
        	Thread.sleep(60000);
        	Thread.sleep(60000);
        	consumer.shutdown();
        	executor.shutdown();
         }
    	
 
    
}
   
}