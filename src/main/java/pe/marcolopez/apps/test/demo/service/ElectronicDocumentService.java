package pe.marcolopez.apps.test.demo.service;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import pe.marcolopez.apps.test.demo.model.ElectronicDocument;

@Slf4j
@ApplicationScoped
public class ElectronicDocumentService {

  @Inject
  Pool client;

  public Uni<ElectronicDocument> findOrCreate(String serialNumber, String invoiceNumber) {
    log.info("### Init for Document Electronic: {}-{}", serialNumber, invoiceNumber);

    String sql = """
          SELECT TOP 1 ID, SERIAL_NUMBER, INVOICE_NUMBER
          FROM dev.ELECTRONIC_DOCUMENT
          WHERE SERIAL_NUMBER = @p1 AND INVOICE_NUMBER = @p2
        """;

    return client
        .preparedQuery(sql)
        .execute(Tuple.of(serialNumber, invoiceNumber))
        .onItem()
        .transformToUni(rows -> {
          if (rows.iterator().hasNext()) {
            ElectronicDocument fetched = from(rows.iterator().next());
            log.info("### ED existing with {}-{}", serialNumber, invoiceNumber);
            return Uni.createFrom().item(fetched);
          } else {
            return insertNewDocument(serialNumber, invoiceNumber);
          }
        });
  }

  private Uni<ElectronicDocument> insertNewDocument(String serialNumber, String invoiceNumber) {
    String sql = """
          INSERT INTO dev.ELECTRONIC_DOCUMENT (SERIAL_NUMBER, INVOICE_NUMBER)
          VALUES (@p1, @p2)
        """;

    return client
        .preparedQuery(sql)
        .execute(Tuple.of(serialNumber, invoiceNumber))
        .onItem()
        .transform(rows -> {
          ElectronicDocument created = ElectronicDocument.builder()
              .serialNumber(serialNumber)
              .invoiceNumber(invoiceNumber)
              .build();
          log.info("### Inserted new document: {}-{}", serialNumber, invoiceNumber);
          return created;
        });
  }

  private ElectronicDocument from(Row row) {
    if (row == null) {
      return null;
    }

    return ElectronicDocument.builder()
        .id(row.getInteger("ID"))
        .serialNumber(row.getString("SERIAL_NUMBER"))
        .invoiceNumber(row.getString("INVOICE_NUMBER"))
        .build();
  }
}
