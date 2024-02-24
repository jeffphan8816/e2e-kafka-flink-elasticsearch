/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package FlinkDE;

import Deserializer.JSONValueDeserializationSchema;
import Dto.SalesPerCategory;
import Dto.SalesPerDay;
import Dto.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcSink;

import java.sql.Date;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main
 * (String[] args))
 * method, change the respective entry in the POM.xml file (simply search for
 * 'mainClass').
 */
public class DataStreamJob {
//    private static final String KAFKA_BROKER = "localhost:9092";
//    private static final String TRANSACTION_TOPIC = "financial_transactions";
    private static final String jdbcUrl = "jdbc:postgresql://localhost:5432/";
    private static final String username = "postgres";
    private static final String password = "postgres";

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "financial_transactions";

        // Create a Kafka consumer
        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic)
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema())
                .build();

        DataStream<Transaction> transactionStream = env.fromSource(source,
                WatermarkStrategy.noWatermarks(), "Kafka Transaction Source");

        // Print the transaction stream to the console
        transactionStream.print();


        // store the stream to Postgres

        // Define the JDBC execution options
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();
        // Define the JDBC connection options
        JdbcConnectionOptions connectionOptions =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(username)
                        .withPassword(password)
                        .build();

        // Create transactions table
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS transactions (" +
                        "transaction_id VARCHAR(255) PRIMARY KEY," +
                        "product_id VARCHAR(255)," +
                        "product_name VARCHAR(255)," +
                        "product_category VARCHAR(255)," +
                        "product_quantity INTEGER," +
                        "product_price DOUBLE PRECISION," +
                        "product_brand VARCHAR(255)," +
                        "total_amount DOUBLE PRECISION," +
                        "customer_id VARCHAR(255)," +
                        "transaction_date TIMESTAMP," +
                        "payment_method VARCHAR(255)," +
                        "currency VARCHAR(255))",
                (JdbcStatementBuilder<Transaction>) (ps, transaction) -> {

                },
                executionOptions,
                connectionOptions
        ));

        // Insert transactions into the table
        transactionStream.addSink(JdbcSink.sink(
                "INSERT INTO transactions (transaction_id, product_id, product_name, " +
                        "product_category, product_quantity, product_price, " +
                        "product_brand, total_amount, customer_id, transaction_date, " +
                        "payment_method, currency) " +
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)" +
                        "ON CONFLICT (transaction_id) DO UPDATE SET " +
                        "product_id = EXCLUDED.product_id, " +
                        "product_name = EXCLUDED.product_name, " +
                        "product_category = EXCLUDED.product_category, " +
                        "product_quantity = EXCLUDED.product_quantity, " +
                        "product_price = EXCLUDED.product_price, " +
                        "product_brand = EXCLUDED.product_brand, " +
                        "total_amount = EXCLUDED.total_amount, " +
                        "customer_id = EXCLUDED.customer_id, " +
                        "transaction_date = EXCLUDED.transaction_date, " +
                        "payment_method = EXCLUDED.payment_method, " +
                        "currency = EXCLUDED.currency",
                (JdbcStatementBuilder<Transaction>) (ps, transaction) -> {
                    ps.setString(1, transaction.getTransactionId());
                    ps.setString(2, transaction.getProductId());
                    ps.setString(3, transaction.getProductName());
                    ps.setString(4, transaction.getProductCategory());
                    ps.setInt(5, transaction.getProductQuantity());
                    ps.setDouble(6, transaction.getProductPrice());
                    ps.setString(7, transaction.getProductBrand());
                    ps.setDouble(8, transaction.getTotalAmount());
                    ps.setString(9, transaction.getCustomerId());
                    ps.setTimestamp(10, transaction.getTransactionDate());
                    ps.setString(11, transaction.getPaymentMethod());
                    ps.setString(12, transaction.getCurrency());
                },
                executionOptions,
                connectionOptions
        )).name("Insert into  transactions table sink");

        // Create sales_per_category table sink
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS sales_per_category (" +
                        "transaction_date DATE," +
                        "product_category VARCHAR(255)," +
                        "total_sales DOUBLE PRECISION, " +
                        "PRIMARY KEY (transaction_date, product_category))",
                (JdbcStatementBuilder<Transaction>) (ps, transaction) -> {

                },
                executionOptions,
                connectionOptions
        ));

        // Create sales_per_day table sink
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS sales_per_day (" +
                        "transaction_date DATE, " +
                        "total_sales DOUBLE PRECISION, " +
                        "PRIMARY KEY (transaction_date))",
                (JdbcStatementBuilder<Transaction>) (ps, transaction) -> {

                },
                executionOptions,
                connectionOptions
        )).name("Create Sales per Day table");

        // Create sales_per_month table sink
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS sales_per_month (" +
                        "year INTEGER, " +
                        "month INTEGER, " +
                        "total_sales DOUBLE PRECISION, " +
                        "PRIMARY KEY (year, month))",
                (JdbcStatementBuilder<Transaction>) (ps, transaction) -> {

                },
                executionOptions,
                connectionOptions
        )).name("Create Sales per Month table");

        transactionStream.map(
                        transaction -> {
                            Date transactionDate = new Date(System.currentTimeMillis());
                            String productCategory = transaction.getProductCategory();
                            double totalSales = transaction.getTotalAmount();
                            return new SalesPerCategory(transactionDate, productCategory, totalSales);
                        }
                ).keyBy(SalesPerCategory::getProductCategory)
                .reduce((salesPerCategory, t1) -> {
                    salesPerCategory.setTotalSales(salesPerCategory.getTotalSales() + t1.getTotalSales());
                    return salesPerCategory;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO sales_per_category (transaction_date, product_category, total_sales) " +
                                "VALUES (?,?,?) " +
                                "ON CONFLICT (transaction_date, product_category) DO UPDATE SET " +
                                "total_sales = EXCLUDED.total_sales" +
                                "where sales_per_category.product_category = EXCLUDED.product_category " +
                                "AND sales_per_category.transaction_date = EXCLUDED.transaction_date" +
                        (JdbcStatementBuilder<SalesPerCategory>) (ps, salesPerCategory) -> {
                            ps.setDate(1, new Date(System.currentTimeMillis()));
                            ps.setString(2, salesPerCategory.getProductCategory());
                            ps.setDouble(3, salesPerCategory.getTotalSales());
                        },
                        (JdbcStatementBuilder<SalesPerCategory>) executionOptions,
                        connectionOptions
                )).name("Insert into sales_per_category table sink");

        transactionStream.map(
                        transaction -> {
                            Date transactionDate = new Date(System.currentTimeMillis());
                            double totalSales = transaction.getTotalAmount();
                            return new SalesPerDay(transactionDate, totalSales);
                        }
                ).keyBy(SalesPerDay::getTransactionDate)
                .reduce((salesPerDay, t1) -> {
                    salesPerDay.setTotalSales(salesPerDay.getTotalSales() + t1.getTotalSales());
                    return salesPerDay;
                }).addSink(JdbcSink.sink(
                        "INSERT INTO sales_per_day (transaction_date, total_sales) " +
                                "VALUES (?,?) " +
                                "ON CONFLICT (transaction_date) DO UPDATE SET " +
                                "total_sales = EXCLUDED.total_sales" +
                                "where sales_per_day.transaction_date = EXCLUDED.transaction_date" +
                                (JdbcStatementBuilder<SalesPerDay>) (ps, salesPerDay) -> {
                                    ps.setDate(1, new Date(System.currentTimeMillis()));
                                    ps.setDouble(2, salesPerDay.getTotalSales());
                                },
                        (JdbcStatementBuilder<SalesPerDay>) executionOptions,
                        connectionOptions
                )).name("Insert into sales_per_day table sink");



        env.execute("Flink Java real-time financial transactions processing job");
    }
}
