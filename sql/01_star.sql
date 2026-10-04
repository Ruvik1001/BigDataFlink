CREATE SCHEMA IF NOT EXISTS star;

CREATE TABLE star.dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE
);

CREATE TABLE star.dim_city (
    city_id    SERIAL PRIMARY KEY,
    city_name  TEXT NOT NULL,
    country_id INTEGER NOT NULL REFERENCES star.dim_country (country_id),
    UNIQUE (city_name, country_id)
);

CREATE TABLE star.dim_pet_type (
    pet_type_id   SERIAL PRIMARY KEY,
    pet_type_name TEXT NOT NULL UNIQUE
);

CREATE TABLE star.dim_pet_breed (
    pet_breed_id SERIAL PRIMARY KEY,
    breed_name   TEXT NOT NULL UNIQUE
);

CREATE TABLE star.dim_product_category (
    category_id   SERIAL PRIMARY KEY,
    category_name TEXT NOT NULL UNIQUE
);

CREATE TABLE star.dim_pet_category (
    pet_category_id   SERIAL PRIMARY KEY,
    pet_category_name TEXT NOT NULL UNIQUE
);

CREATE TABLE star.dim_brand (
    brand_id   SERIAL PRIMARY KEY,
    brand_name TEXT NOT NULL UNIQUE
);

CREATE TABLE star.dim_color (
    color_id   SERIAL PRIMARY KEY,
    color_name TEXT NOT NULL UNIQUE
);

CREATE TABLE star.dim_size (
    size_id   SERIAL PRIMARY KEY,
    size_name TEXT NOT NULL UNIQUE
);

CREATE TABLE star.dim_material (
    material_id   SERIAL PRIMARY KEY,
    material_name TEXT NOT NULL UNIQUE
);

CREATE TABLE star.dim_date (
    date_id     SERIAL PRIMARY KEY,
    full_date   DATE NOT NULL UNIQUE,
    year_num    INTEGER NOT NULL,
    quarter_num INTEGER NOT NULL,
    month_num   INTEGER NOT NULL,
    day_num     INTEGER NOT NULL,
    month_name  TEXT NOT NULL
);

CREATE TABLE star.dim_supplier (
    supplier_id   SERIAL PRIMARY KEY,
    supplier_name TEXT,
    contact_name  TEXT,
    email         TEXT NOT NULL UNIQUE,
    phone         TEXT,
    address       TEXT,
    city_id       INTEGER REFERENCES star.dim_city (city_id)
);

CREATE TABLE star.dim_customer (
    customer_id  SERIAL PRIMARY KEY,
    first_name   TEXT,
    last_name    TEXT,
    age          INTEGER,
    email        TEXT NOT NULL UNIQUE,
    postal_code  TEXT,
    country_id   INTEGER REFERENCES star.dim_country (country_id),
    pet_type_id  INTEGER REFERENCES star.dim_pet_type (pet_type_id),
    pet_breed_id INTEGER REFERENCES star.dim_pet_breed (pet_breed_id),
    pet_name     TEXT
);

CREATE TABLE star.dim_seller (
    seller_id   SERIAL PRIMARY KEY,
    first_name  TEXT,
    last_name   TEXT,
    email       TEXT NOT NULL UNIQUE,
    postal_code TEXT,
    country_id  INTEGER REFERENCES star.dim_country (country_id)
);

CREATE TABLE star.dim_store (
    store_id   SERIAL PRIMARY KEY,
    store_name TEXT,
    location   TEXT,
    state_name TEXT,
    phone      TEXT,
    email      TEXT NOT NULL UNIQUE,
    city_id    INTEGER REFERENCES star.dim_city (city_id)
);

CREATE TABLE star.dim_product (
    product_id      SERIAL PRIMARY KEY,
    product_name    TEXT,
    price           NUMERIC(12, 2),
    stock_quantity  INTEGER,
    weight          NUMERIC(12, 2),
    rating          NUMERIC(4, 2),
    reviews         INTEGER,
    description     TEXT,
    release_date    DATE,
    expiry_date     DATE,
    category_id     INTEGER REFERENCES star.dim_product_category (category_id),
    pet_category_id INTEGER REFERENCES star.dim_pet_category (pet_category_id),
    brand_id        INTEGER REFERENCES star.dim_brand (brand_id),
    color_id        INTEGER REFERENCES star.dim_color (color_id),
    size_id         INTEGER REFERENCES star.dim_size (size_id),
    material_id     INTEGER REFERENCES star.dim_material (material_id),
    supplier_id     INTEGER REFERENCES star.dim_supplier (supplier_id),
    -- sale_product_id в csv не глобальный, ключ по атрибутам
    product_nk      TEXT NOT NULL UNIQUE
);

CREATE TABLE star.fact_sales (
    sale_id          BIGSERIAL PRIMARY KEY,
    sale_date_id     INTEGER NOT NULL REFERENCES star.dim_date (date_id),
    customer_id      INTEGER NOT NULL REFERENCES star.dim_customer (customer_id),
    seller_id        INTEGER NOT NULL REFERENCES star.dim_seller (seller_id),
    product_id       INTEGER NOT NULL REFERENCES star.dim_product (product_id),
    store_id         INTEGER NOT NULL REFERENCES star.dim_store (store_id),
    sale_quantity    INTEGER,
    sale_total_price NUMERIC(14, 2),
    source_file      TEXT NOT NULL,
    source_row       INTEGER NOT NULL,
    UNIQUE (source_file, source_row)
);
