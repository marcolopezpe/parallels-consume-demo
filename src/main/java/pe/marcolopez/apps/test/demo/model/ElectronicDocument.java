package pe.marcolopez.apps.test.demo.model;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@Builder
public class ElectronicDocument {

  private Integer id;
  private String serialNumber;
  private String invoiceNumber;
  private String subscriberId;
  private LocalDate emissionDate;
  private LocalDate expirationDate;
  private BigDecimal igv;
  private BigDecimal total;
  private String action;
  private Integer version;
  private String xmlLink;
  private String pdfLink;
  private LocalDateTime createdDate;
  private LocalDateTime updatedDate;
}
