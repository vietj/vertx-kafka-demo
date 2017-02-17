package io.vertx.kafkademo;

import io.debezium.kafka.KafkaCluster;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class Main {

  public static void main(String[] args) throws Exception {

    Vertx vertx = Vertx.vertx();

    Router router = Router.router(vertx);

    BridgeOptions options = new BridgeOptions();
    options.setOutboundPermitted(Collections.singletonList(new PermittedOptions().setAddress("dashboard")));
    router.route("/eventbus/*").handler(SockJSHandler.create(vertx).bridge(options));
    router.route().handler(StaticHandler.create().setCachingEnabled(false));

    HttpServer httpServer = vertx.createHttpServer();
    httpServer.requestHandler(router::accept).listen(8080, ar -> {
      if (ar.succeeded()) {
        System.out.println("Http server started");
      } else {
        ar.cause().printStackTrace();
      }
    });

    // Kafka setup for the demo
    KafkaCluster kafkaCluster = new KafkaCluster().withPorts(2181, 9092).addBrokers(1).startup();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        kafkaCluster.shutdown();
      }
    });

    // Create the kafka consumer that send to browser via event bus
    Properties consumerConfig = kafkaCluster.useTo().getConsumerProperties("the_group", "the_client", OffsetResetStrategy.LATEST);
    KafkaReadStream<String, String> consumer = KafkaReadStream.create(vertx, consumerConfig, String.class, String.class);
    consumer.handler(record -> {
      vertx.eventBus().publish("dashboard", record.value());
    });
    consumer.subscribe(Collections.singleton("the_topic"));

    // Create producer for the demo (can be removed if using an external Kafka broker)
    Properties producerConfig = kafkaCluster.useTo().getProducerProperties("the_producer");
    KafkaWriteStream<String ,String> producer = KafkaWriteStream.create(vertx, producerConfig, String.class, String.class);
    vertx.setPeriodic(1000, id -> {
      producer.write(new ProducerRecord<>("the_topic", new JsonObject().put("Hello", "World").encode()), ar -> {
        if (ar.failed()) {
          ar.cause().printStackTrace();
        }
      });
    });
  }
}
