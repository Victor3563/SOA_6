CREATE KEYSPACE IF NOT EXISTS warehouse
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Остаток товара в конкретной зоне (точечный запрос по product + zone)
CREATE TABLE IF NOT EXISTS warehouse.inventory_by_product_zone (
    product_id text,
    zone_id text,
    available_quantity int,
    reserved_quantity int,
    updated_at timestamp,
    PRIMARY KEY ((product_id, zone_id))
);

-- Все зоны, где есть товар (partition = product_id)
CREATE TABLE IF NOT EXISTS warehouse.inventory_by_product (
    product_id text,
    zone_id text,
    available_quantity int,
    reserved_quantity int,
    updated_at timestamp,
    PRIMARY KEY (product_id, zone_id)
);

-- Все товары в зоне (partition = zone_id)
CREATE TABLE IF NOT EXISTS warehouse.inventory_by_zone (
    zone_id text,
    product_id text,
    available_quantity int,
    reserved_quantity int,
    updated_at timestamp,
    PRIMARY KEY (zone_id, product_id)
);

-- Идемпотентность: уже обработанные event_id
CREATE TABLE IF NOT EXISTS warehouse.processed_events (
    event_id text,
    event_type text,
    kafka_partition int,
    kafka_offset bigint,
    processed_at timestamp,
    PRIMARY KEY (event_id)
);

-- Заказы
CREATE TABLE IF NOT EXISTS warehouse.orders (
    order_id text,
    status text,
    items_json text,
    created_at timestamp,
    completed_at timestamp,
    PRIMARY KEY (order_id)
);

-- Аудит: история событий
CREATE TABLE IF NOT EXISTS warehouse.event_history (
    event_id text,
    event_type text,
    product_id text,
    zone_id text,
    quantity int,
    event_timestamp timestamp,
    processed_at timestamp,
    payload_json text,
    PRIMARY KEY (event_id)
);
