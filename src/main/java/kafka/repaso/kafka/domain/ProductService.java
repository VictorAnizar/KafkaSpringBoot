package kafka.repaso.kafka.domain;
import kafka.repaso.kafka.domain.CreateProductRestModel;

public interface ProductService {
    String createProduct(CreateProductRestModel productRestModel) throws Exception;
}
