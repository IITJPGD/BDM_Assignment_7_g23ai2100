
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bson.Document;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Accumulators;

public class Main {

    private static final String DELIMITER = "\\|";
    private static final String CUSTOMER_FILE = "data/customer.tbl";
    private static final String ORDER_FILE = "data/order.tbl";

    // Utility method to parse file and create documents
    private List<Document> parseFile(String filePath, DocumentParser parser) throws IOException {
        List<Document> documents = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(DELIMITER);
                Optional<Document> document = parser.parse(fields);
                document.ifPresent(documents::add);
            }
        }
        return documents;
    }

    // Functional interface for parsing different types of documents
    @FunctionalInterface
    private interface DocumentParser {

        Optional<Document> parse(String[] fields);
    }

    // Ans 1
    public void load() throws Exception {
        MongoCollection<Document> customerCol = db.getCollection("customer");
        MongoCollection<Document> ordersCol = db.getCollection("orders");

        // Clear existing data
        customerCol.drop();
        ordersCol.drop();

        // Parse customer documents
        List<Document> customers = parseFile(CUSTOMER_FILE, fields -> {
            if (fields.length < 4) {
                return Optional.empty();
            }
            return Optional.of(new Document("custkey", Integer.parseInt(fields[0]))
                    .append("name", fields[1])
                    .append("address", fields[2])
                    .append("nationkey", Integer.parseInt(fields[3])));
        });

        // Parse order documents
        List<Document> orders = parseFile(ORDER_FILE, fields -> {
            if (fields.length < 4) {
                return Optional.empty();
            }
            return Optional.of(new Document("orderkey", Integer.parseInt(fields[0]))
                    .append("custkey", Integer.parseInt(fields[1]))
                    .append("orderdate", fields[2])
                    .append("totalprice", Double.parseDouble(fields[3])));
        });

        // Bulk insert
        if (!customers.isEmpty()) {
            customerCol.insertMany(customers);
        }
        if (!orders.isEmpty()) {
            ordersCol.insertMany(orders);
        }

        System.out.println("Data loaded successfully into MongoDB.");
    }

    // Ans 2
    public void loadNest() throws Exception {
        MongoCollection<Document> custOrdersCol = db.getCollection("custorders");
        custOrdersCol.drop();
        MongoCollection<Document> customerCol = db.getCollection("customer");
        MongoCollection<Document> ordersCol = db.getCollection("orders");

        List<Document> nestedCustomers = customerCol.find().map(customer -> {
            List<Document> customerOrders = ordersCol.find(Filters.eq("custkey", customer.getInteger("custkey")))
                    .into(new ArrayList<>());
            customer.append("orders", customerOrders);
            return customer;
        }).into(new ArrayList<>());

        if (!nestedCustomers.isEmpty()) {
            custOrdersCol.insertMany(nestedCustomers);
        }
    }

    // Ans 3
    public String query1(int custkey) {
        MongoCollection<Document> customerCol = db.getCollection("customer");
        Document customer = customerCol.find(Filters.eq("custkey", custkey)).first();
        return customer != null ? customer.getString("name") : null;
    }

    // Ans 4
    public String query2(int orderId) {
        MongoCollection<Document> ordersCol = db.getCollection("orders");
        Document order = ordersCol.find(Filters.eq("orderkey", orderId)).first();
        return order != null ? order.getString("orderdate") : null;
    }

    // Ans 5
    public String query2Nest(int orderId) {
        MongoCollection<Document> custOrdersCol = db.getCollection("custorders");
        Document custOrder = custOrdersCol.find(Filters.eq("orders.orderkey", orderId)).first();

        if (custOrder != null && custOrder.get("orders") instanceof List<?> ordersList) {
            @SuppressWarnings("unchecked")
            List<Document> orders = (List<Document>) ordersList;
            return orders.stream()
                    .filter(order -> order.getInteger("orderkey") == orderId)
                    .findFirst()
                    .map(order -> order.getString("orderdate"))
                    .orElse(null);
        }
        return null;
    }

    // Ans 6
    public long query3() {
        MongoCollection<Document> ordersCol = db.getCollection("orders");
        return ordersCol.countDocuments();
    }

    // Ans 7
    public long query3Nest() {
        MongoCollection<Document> custOrdersCol = db.getCollection("custorders");
        return custOrdersCol.find()
                .map(doc -> {
                    Object ordersObj = doc.get("orders");
                    return ordersObj instanceof List<?> ? ((List<?>) ordersObj).size() : 0;
                })
                .reduce(0L, Long::sum);
    }

    // Ans 8
    public List<Document> query4() {
        MongoCollection<Document> ordersCol = db.getCollection("orders");
        MongoCollection<Document> customerCol = db.getCollection("customer");

        return customerCol.find().map(customer -> {
            int custkey = customer.getInteger("custkey");
            List<Document> pipeline = Arrays.asList(
                    Aggregates.match(Filters.eq("custkey", custkey)),
                    Aggregates.group("$custkey", Accumulators.sum("total", "$totalprice"))
            );

            Document aggResult = ordersCol.aggregate(pipeline).first();
            double total = aggResult != null ? aggResult.getDouble("total") : 0.0;

            customer.append("totalOrderAmount", total);
            return customer;
        })
                .into(new ArrayList<>())
                .stream()
                .sorted((d1, d2) -> Double.compare(d2.getDouble("totalOrderAmount"),
                d1.getDouble("totalOrderAmount")))
                .limit(5)
                .collect(Collectors.toList());
    }

    // Ans 9
    public List<Document> query4Nest() {
        MongoCollection<Document> custOrdersCol = db.getCollection("custorders");

        return custOrdersCol.find()
                .map(custOrder -> {
                    double totalOrderAmount = Optional.ofNullable(custOrder.get("orders"))
                            .filter(obj -> obj instanceof List<?>)
                            .map(obj -> ((List<Document>) obj).stream()
                            .mapToDouble(order -> order.getDouble("totalprice"))
                            .sum())
                            .orElse(0.0);

                    custOrder.append("totalOrderAmount", totalOrderAmount);
                    return custOrder;
                })
                .into(new ArrayList<>())
                .stream()
                .sorted((d1, d2) -> Double.compare(d2.getDouble("totalOrderAmount"),
                d1.getDouble("totalOrderAmount")))
                .limit(5)
                .collect(Collectors.toList());
    }
}
