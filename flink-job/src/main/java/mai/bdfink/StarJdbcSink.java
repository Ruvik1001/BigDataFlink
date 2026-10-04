package mai.bdfink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class StarJdbcSink extends RichSinkFunction<String> {

    private static final DateTimeFormatter CSV_DATE = DateTimeFormatter.ofPattern("M/d/yyyy");
    private static final DateTimeFormatter MONTH = DateTimeFormatter.ofPattern("MMMM", Locale.ENGLISH);
    private static final ObjectMapper JSON = new ObjectMapper();

    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://" + env("POSTGRES_HOST") + ":"
                + env("POSTGRES_INTERNAL_PORT") + "/" + env("POSTGRES_DB");
        conn = DriverManager.getConnection(url, env("POSTGRES_USER"), env("POSTGRES_PASSWORD"));
        conn.setAutoCommit(false);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        JsonNode row = JSON.readTree(value);
        String sourceFile = text(row, "source_file");
        int sourceRow = row.get("source_row").asInt();
        try {
            Integer countryCustomer = country(text(row, "customer_country"));
            Integer countrySeller = country(text(row, "seller_country"));
            Integer countryStore = country(text(row, "store_country"));
            Integer countrySupplier = country(text(row, "supplier_country"));

            Integer cityStore = city(text(row, "store_city"), countryStore);
            Integer citySupplier = city(text(row, "supplier_city"), countrySupplier);

            Integer petType = named("star.dim_pet_type", "pet_type_id", "pet_type_name", text(row, "customer_pet_type"));
            Integer breed = named("star.dim_pet_breed", "pet_breed_id", "breed_name", text(row, "customer_pet_breed"));
            Integer category = named("star.dim_product_category", "category_id", "category_name", text(row, "product_category"));
            Integer petCategory = named("star.dim_pet_category", "pet_category_id", "pet_category_name", text(row, "pet_category"));
            Integer brand = named("star.dim_brand", "brand_id", "brand_name", text(row, "product_brand"));
            Integer color = named("star.dim_color", "color_id", "color_name", text(row, "product_color"));
            Integer size = named("star.dim_size", "size_id", "size_name", text(row, "product_size"));
            Integer material = named("star.dim_material", "material_id", "material_name", text(row, "product_material"));

            Integer dateId = saleDate(text(row, "sale_date"));
            Integer supplierId = supplier(row, citySupplier);
            Integer customerId = customer(row, countryCustomer, petType, breed);
            Integer sellerId = seller(row, countrySeller);
            Integer storeId = store(row, cityStore);
            Integer productId = product(row, category, petCategory, brand, color, size, material, supplierId);

            if (dateId == null || customerId == null || sellerId == null || productId == null || storeId == null) {
                throw new IllegalStateException("нет измерения для " + sourceFile + ":" + sourceRow);
            }

            insertFact(dateId, customerId, sellerId, productId, storeId, row, sourceFile, sourceRow);
            conn.commit();
        } catch (Exception ex) {
            conn.rollback();
            throw ex;
        }
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    private Integer country(String name) throws SQLException {
        if (name == null) {
            return null;
        }
        return id(
                "WITH ins AS ("
                        + " INSERT INTO star.dim_country (country_name) VALUES (?)"
                        + " ON CONFLICT (country_name) DO NOTHING RETURNING country_id"
                        + ") SELECT country_id FROM ins"
                        + " UNION ALL SELECT country_id FROM star.dim_country WHERE country_name = ?"
                        + " LIMIT 1",
                name, name);
    }

    private Integer city(String name, Integer countryId) throws SQLException {
        if (name == null || countryId == null) {
            return null;
        }
        return id(
                "WITH ins AS ("
                        + " INSERT INTO star.dim_city (city_name, country_id) VALUES (?, ?)"
                        + " ON CONFLICT (city_name, country_id) DO NOTHING RETURNING city_id"
                        + ") SELECT city_id FROM ins"
                        + " UNION ALL SELECT city_id FROM star.dim_city WHERE city_name = ? AND country_id = ?"
                        + " LIMIT 1",
                name, countryId, name, countryId);
    }

    private Integer named(String table, String idCol, String nameCol, String value) throws SQLException {
        if (value == null) {
            return null;
        }
        String sql = "WITH ins AS ("
                + " INSERT INTO " + table + " (" + nameCol + ") VALUES (?)"
                + " ON CONFLICT (" + nameCol + ") DO NOTHING RETURNING " + idCol
                + ") SELECT " + idCol + " FROM ins"
                + " UNION ALL SELECT " + idCol + " FROM " + table + " WHERE " + nameCol + " = ?"
                + " LIMIT 1";
        return id(sql, value, value);
    }

    private Integer saleDate(String raw) throws SQLException {
        LocalDate date = parseDate(raw);
        if (date == null) {
            return null;
        }
        int quarter = (date.getMonthValue() - 1) / 3 + 1;
        return id(
                "WITH ins AS ("
                        + " INSERT INTO star.dim_date (full_date, year_num, quarter_num, month_num, day_num, month_name)"
                        + " VALUES (?, ?, ?, ?, ?, ?)"
                        + " ON CONFLICT (full_date) DO NOTHING RETURNING date_id"
                        + ") SELECT date_id FROM ins"
                        + " UNION ALL SELECT date_id FROM star.dim_date WHERE full_date = ?"
                        + " LIMIT 1",
                Date.valueOf(date),
                date.getYear(),
                quarter,
                date.getMonthValue(),
                date.getDayOfMonth(),
                MONTH.format(date),
                Date.valueOf(date));
    }

    private Integer supplier(JsonNode row, Integer cityId) throws SQLException {
        String email = text(row, "supplier_email");
        if (email == null) {
            return null;
        }
        return id(
                "WITH ins AS ("
                        + " INSERT INTO star.dim_supplier (supplier_name, contact_name, email, phone, address, city_id)"
                        + " VALUES (?, ?, ?, ?, ?, ?)"
                        + " ON CONFLICT (email) DO NOTHING RETURNING supplier_id"
                        + ") SELECT supplier_id FROM ins"
                        + " UNION ALL SELECT supplier_id FROM star.dim_supplier WHERE email = ?"
                        + " LIMIT 1",
                text(row, "supplier_name"),
                text(row, "supplier_contact"),
                email,
                text(row, "supplier_phone"),
                text(row, "supplier_address"),
                cityId,
                email);
    }

    private Integer customer(JsonNode row, Integer countryId, Integer petTypeId, Integer breedId) throws SQLException {
        String email = text(row, "customer_email");
        if (email == null) {
            return null;
        }
        return id(
                "WITH ins AS ("
                        + " INSERT INTO star.dim_customer"
                        + " (first_name, last_name, age, email, postal_code, country_id, pet_type_id, pet_breed_id, pet_name)"
                        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                        + " ON CONFLICT (email) DO NOTHING RETURNING customer_id"
                        + ") SELECT customer_id FROM ins"
                        + " UNION ALL SELECT customer_id FROM star.dim_customer WHERE email = ?"
                        + " LIMIT 1",
                text(row, "customer_first_name"),
                text(row, "customer_last_name"),
                intOrNull(text(row, "customer_age")),
                email,
                text(row, "customer_postal_code"),
                countryId,
                petTypeId,
                breedId,
                text(row, "customer_pet_name"),
                email);
    }

    private Integer seller(JsonNode row, Integer countryId) throws SQLException {
        String email = text(row, "seller_email");
        if (email == null) {
            return null;
        }
        return id(
                "WITH ins AS ("
                        + " INSERT INTO star.dim_seller (first_name, last_name, email, postal_code, country_id)"
                        + " VALUES (?, ?, ?, ?, ?)"
                        + " ON CONFLICT (email) DO NOTHING RETURNING seller_id"
                        + ") SELECT seller_id FROM ins"
                        + " UNION ALL SELECT seller_id FROM star.dim_seller WHERE email = ?"
                        + " LIMIT 1",
                text(row, "seller_first_name"),
                text(row, "seller_last_name"),
                email,
                text(row, "seller_postal_code"),
                countryId,
                email);
    }

    private Integer store(JsonNode row, Integer cityId) throws SQLException {
        String email = text(row, "store_email");
        if (email == null) {
            return null;
        }
        return id(
                "WITH ins AS ("
                        + " INSERT INTO star.dim_store (store_name, location, state_name, phone, email, city_id)"
                        + " VALUES (?, ?, ?, ?, ?, ?)"
                        + " ON CONFLICT (email) DO NOTHING RETURNING store_id"
                        + ") SELECT store_id FROM ins"
                        + " UNION ALL SELECT store_id FROM star.dim_store WHERE email = ?"
                        + " LIMIT 1",
                text(row, "store_name"),
                text(row, "store_location"),
                text(row, "store_state"),
                text(row, "store_phone"),
                email,
                cityId,
                email);
    }

    private Integer product(JsonNode row, Integer categoryId, Integer petCategoryId, Integer brandId,
                            Integer colorId, Integer sizeId, Integer materialId, Integer supplierId) throws SQLException {
        String nk = productNk(row);
        return id(
                "WITH ins AS ("
                        + " INSERT INTO star.dim_product ("
                        + " product_name, price, stock_quantity, weight, rating, reviews, description,"
                        + " release_date, expiry_date, category_id, pet_category_id, brand_id, color_id,"
                        + " size_id, material_id, supplier_id, product_nk"
                        + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                        + " ON CONFLICT (product_nk) DO NOTHING RETURNING product_id"
                        + ") SELECT product_id FROM ins"
                        + " UNION ALL SELECT product_id FROM star.dim_product WHERE product_nk = ?"
                        + " LIMIT 1",
                text(row, "product_name"),
                decimal(text(row, "product_price")),
                intOrNull(text(row, "product_quantity")),
                decimal(text(row, "product_weight")),
                decimal(text(row, "product_rating")),
                intOrNull(text(row, "product_reviews")),
                text(row, "product_description"),
                sqlDate(text(row, "product_release_date")),
                sqlDate(text(row, "product_expiry_date")),
                categoryId,
                petCategoryId,
                brandId,
                colorId,
                sizeId,
                materialId,
                supplierId,
                nk,
                nk);
    }

    private void insertFact(int dateId, int customerId, int sellerId, int productId, int storeId,
                            JsonNode row, String sourceFile, int sourceRow) throws SQLException {
        String sql = "INSERT INTO star.fact_sales ("
                + " sale_date_id, customer_id, seller_id, product_id, store_id,"
                + " sale_quantity, sale_total_price, source_file, source_row"
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                + " ON CONFLICT (source_file, source_row) DO NOTHING";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, dateId);
            ps.setInt(2, customerId);
            ps.setInt(3, sellerId);
            ps.setInt(4, productId);
            ps.setInt(5, storeId);
            setObject(ps, 6, intOrNull(text(row, "sale_quantity")), Types.INTEGER);
            setObject(ps, 7, decimal(text(row, "sale_total_price")), Types.NUMERIC);
            ps.setString(8, sourceFile);
            ps.setInt(9, sourceRow);
            ps.executeUpdate();
        }
    }

    private Integer id(String sql, Object... params) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                setObject(ps, i + 1, params[i], Types.OTHER);
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                int value = rs.getInt(1);
                return rs.wasNull() ? null : value;
            }
        }
    }

    private static void setObject(PreparedStatement ps, int index, Object value, int sqlType) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.NULL);
            return;
        }
        if (value instanceof Date) {
            ps.setDate(index, (Date) value);
            return;
        }
        if (value instanceof Integer) {
            ps.setInt(index, (Integer) value);
            return;
        }
        if (value instanceof java.math.BigDecimal) {
            ps.setBigDecimal(index, (java.math.BigDecimal) value);
            return;
        }
        ps.setString(index, value.toString());
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        String raw = value.asText().trim();
        return raw.isEmpty() ? null : raw;
    }

    private static String nz(String value) {
        return value == null ? "" : value;
    }

    private static Integer intOrNull(String value) {
        if (value == null) {
            return null;
        }
        return Integer.valueOf(value);
    }

    private static java.math.BigDecimal decimal(String value) {
        if (value == null) {
            return null;
        }
        return new java.math.BigDecimal(value);
    }

    private static LocalDate parseDate(String value) {
        if (value == null) {
            return null;
        }
        return LocalDate.parse(value, CSV_DATE);
    }

    private static Date sqlDate(String value) {
        LocalDate date = parseDate(value);
        return date == null ? null : Date.valueOf(date);
    }

    // тот же набор полей, что в Spark-лабе: id из csv между файлами совпадают
    private static String productNk(JsonNode row) {
        String raw = String.join("|",
                nz(text(row, "product_name")),
                nz(text(row, "product_category")),
                nz(text(row, "product_price")),
                nz(text(row, "product_quantity")),
                nz(text(row, "pet_category")),
                nz(text(row, "product_weight")),
                nz(text(row, "product_color")),
                nz(text(row, "product_size")),
                nz(text(row, "product_brand")),
                nz(text(row, "product_material")),
                nz(text(row, "product_description")),
                nz(text(row, "product_rating")),
                nz(text(row, "product_reviews")),
                nz(text(row, "product_release_date")),
                nz(text(row, "product_expiry_date")),
                nz(text(row, "supplier_email")));
        try {
            byte[] digest = MessageDigest.getInstance("MD5").digest(raw.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static String env(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("нет переменной " + key);
        }
        return value;
    }
}
