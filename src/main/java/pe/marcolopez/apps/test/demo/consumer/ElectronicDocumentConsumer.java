package pe.marcolopez.apps.test.demo.consumer;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import pe.marcolopez.apps.test.demo.model.ElectronicDocument;
import pe.marcolopez.apps.test.demo.service.ElectronicDocumentService;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
@ApplicationScoped
public class ElectronicDocumentConsumer {

  @ConfigProperty(name = "app.kafka.topic", defaultValue = "topic")
  String topic;

  @Inject
  Vertx vertx;

  @Inject
  Consumer<String, String> consumer;

  @Inject
  ElectronicDocumentService electronicDocumentService;

  public void initialize(@Observes StartupEvent ev) {
    log.info("### Initializing ElectronicDocumentConsumer...");
    consumer.subscribe(Collections.singleton(topic));

    vertx.setPeriodic(1000, id -> {
      vertx.executeBlocking(Uni.createFrom().item(() ->
              consumer.poll(Duration.ofSeconds(1))))
          .subscribe()
          .with(consumerRecords -> {
            log.info("### Consumer Records with UUID: {}", UUID.randomUUID());
            processRecords(consumerRecords);
          });
    });
  }

  private void processRecords(ConsumerRecords<String, String> records) {
    Multi.createFrom().iterable(records)
        .onItem().transformToUniAndMerge(this::processRecord)
        .subscribe().with(
            success -> log.info("### Success..."),
            failure -> log.info("### Failure...", failure)
        );
  }

  private Uni<Void> processRecord(ConsumerRecord<String, String> record) {
    String serialNumber = record.value().split("-")[0];
    String invoiceNumber = record.value().split("-")[1];
    return electronicDocumentService
        .findOrCreate(serialNumber, invoiceNumber)
        .replaceWithVoid();
  }
}
