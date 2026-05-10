# Smart Warehouse — задание на 4 балла

## Таблицы Cassandra и зачем такие ключи

Данные по остаткам **продублированы в трёх таблицах**

| Таблица | Partition key | Clustering | Какой запрос |
|---------|---------------|------------|--------------|
| `inventory_by_product_zone` | `(product_id, zone_id)` | — | Сколько товара в конкретной зоне |
| `inventory_by_product` | `product_id` | `zone_id` | Все зоны, где лежит товар |
| `inventory_by_zone` | `zone_id` | `product_id` | Все товары в зоне |

Служебные таблицы:

- `processed_events` — какие `event_id` уже обработаны (идемпотентность).
- `orders` — заказы и их статус.
- `event_history` — аудит: что пришло и когда обработали.

---

## Запуск

```bash
cd SOA_6
docker compose up --build -d
```

Открыть логи consumer

```bash
docker compose logs -f consumer
```

Ожидаемая строка при старте:

`Consumer started: group=warehouse-state-consumer topic=warehouse-events`


---

### Шаг 1. Приёмка товара (пункты 1–3)

```bash
docker compose run --rm producer \
  --event-type PRODUCT_RECEIVED \
  --product-id SKU-001 \
  --zone-id ZONE-A \
  --quantity 100
```


```bash
docker compose exec -T cassandra cqlsh -e \
  "SELECT * FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"

docker compose exec -T cassandra cqlsh -e \
  "SELECT * FROM warehouse.inventory_by_product WHERE product_id='SKU-001';"
```
---

### Шаг 2. Резерв и перемещение

```bash
docker compose run --rm producer \
  --event-type PRODUCT_RESERVED \
  --product-id SKU-001 \
  --zone-id ZONE-A \
  --quantity 30
```

```bash
docker compose exec -T cassandra cqlsh -e \
  "SELECT zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product WHERE product_id='SKU-001';"
```

```bash
docker compose run --rm producer \
  --event-type PRODUCT_MOVED \
  --product-id SKU-001 \
  --from-zone ZONE-A \
  --to-zone ZONE-B \
  --quantity 20
```

```bash
docker compose exec -T cassandra cqlsh -e \
  "SELECT zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product WHERE product_id='SKU-001';"
```

---

### Шаг 3. Заказ (ORDER_CREATED → ORDER_COMPLETED)


```bash
docker compose run --rm producer \
  --event-type ORDER_CREATED \
  --order-id order-demo-1 \
  --order-items-json '[{"product_id":"SKU-001","zone_id":"ZONE-A","quantity":15}]'

docker compose run --rm producer \
  --event-type ORDER_COMPLETED \
  --order-id order-demo-1
```

```bash
docker compose exec -T cassandra cqlsh -e \
  "SELECT order_id, status FROM warehouse.orders WHERE order_id='order-demo-1';"

docker compose exec -T cassandra cqlsh -e \
  "SELECT zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product WHERE product_id='SKU-001';"
```


---

### Шаг 4. Идемпотентность (4 балл)

```bash
docker compose run --rm producer \
  --event-type PRODUCT_RECEIVED \
  --event-id demo-idem-001 \
  --product-id SKU-002 \
  --zone-id ZONE-A \
  --quantity 50

docker compose run --rm producer \
  --event-type PRODUCT_RECEIVED \
  --event-id demo-idem-001 \
  --product-id SKU-002 \
  --zone-id ZONE-A \
  --quantity 50
```

```bash
docker compose exec -T cassandra cqlsh -e \
  "SELECT available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-002' AND zone_id='ZONE-A';"

docker compose exec -T cassandra cqlsh -e \
  "SELECT event_id, event_type FROM warehouse.processed_events WHERE event_id='demo-idem-001';"
```

