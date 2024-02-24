package Dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Date;
@Data
@AllArgsConstructor
public class SalesPerCategory {
    private Date transactionDate;
    private String productCategory;
    private double totalSales;
}
