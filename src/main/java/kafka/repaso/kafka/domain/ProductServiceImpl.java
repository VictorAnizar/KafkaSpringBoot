package kafka.repaso.kafka.domain;

import kafka.repaso.kafka.domain.events.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import kafka.repaso.kafka.domain.CreateProductRestModel;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class ProductServiceImpl implements ProductService{

    /*
    This class was dessigned to send messages or to publish evenets to kafka topic

    As a message in kafka is a key vaule pair for example the data type for the key is String
    and the data type of the object will be ProductCreatedEvent
    * */
    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate){
        this.kafkaTemplate=kafkaTemplate;
    }

    public String createProduct(CreateProductRestModel productRestModel) throws Exception{
        String productId = UUID.randomUUID().toString();
        // TODO: persist product details into a db before publishing an event

        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId, productRestModel.getTitle(),
                productRestModel.getPrice(), productRestModel.getQuantity());

        //This will send a message to a kafka topic asrynchronously and it WONT wait
        // for acknowledgement that the message was succesfully persisted
        //CompletableFuture<SendResult<String, ProductCreatedEvent>> future = kafkaTemplate.send("products-created-events-topic", productId, productCreatedEvent);

        /*future.whenComplete((result, exception)->{
           if(exception!=null){
                LOGGER.error("*************** Failed sending message: "+exception.getMessage());
           }else{
                LOGGER.info("*************** Succesfully sent: "+result.getRecordMetadata());
           }
        });*/

        // If add this line the code is synchronous because wait unitl all the promises completed
        //future.join();
        //However if we dont want to use Completable future
        //If we want some acknowledgment when reciving messages we can implement this code in a
        //Synchronously way

        LOGGER.info("Before sending message------------");

        SendResult<String, ProductCreatedEvent> result = kafkaTemplate
                .send("products-created-events-topic", productId, productCreatedEvent).get();

        LOGGER.info("Partition: "+result.getRecordMetadata().partition());
        LOGGER.info("Topic: "+result.getRecordMetadata().topic());


        LOGGER.info("************** Returning product id ");

        return productId;
    }
}
