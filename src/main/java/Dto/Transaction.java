package Dto;

import lombok.Data;

import java.sql.Timestamp;
@Data
public class Transaction {
    private String transactionId;
    private String productId;
    private String productName;
    private String productCategory;
    private double productPrice;
    private int productQuantity;
    private double totalAmount;
    private String productBrand;
    private String currency;
    private String customerId;
    private Timestamp transactionDate;
    private String paymentMethod;
}
