package cassandra

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/victor3563/soa6/internal/events"
)

type Repository struct {
	session *gocql.Session
}

func New(hosts []string, keyspace string) (*Repository, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.One
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &Repository{session: session}, nil
}

func (r *Repository) Close() {
	r.session.Close()
}

func (r *Repository) Ping() error {
	return r.session.Query("SELECT now() FROM system.local").Exec()
}

func (r *Repository) IsEventProcessed(eventID string) (bool, error) {
	var id string
	err := r.session.Query(
		"SELECT event_id FROM processed_events WHERE event_id = ?",
		eventID,
	).Scan(&id)
	if err == gocql.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *Repository) MarkEventProcessed(eventID, eventType string, partition int32, offset int64) error {
	return r.session.Query(
		"INSERT INTO processed_events (event_id, event_type, kafka_partition, kafka_offset, processed_at) VALUES (?, ?, ?, ?, ?)",
		eventID, eventType, partition, offset, time.Now().UTC(),
	).Exec()
}

func (r *Repository) GetInventory(productID, zoneID string) (available, reserved int, err error) {
	err = r.session.Query(
		"SELECT available_quantity, reserved_quantity FROM inventory_by_product_zone WHERE product_id = ? AND zone_id = ?",
		productID, zoneID,
	).Scan(&available, &reserved)
	if err == gocql.ErrNotFound {
		return 0, 0, nil
	}
	return available, reserved, err
}

func (r *Repository) WriteInventory(productID, zoneID string, available, reserved int) error {
	now := time.Now().UTC()
	if err := r.session.Query(
		"INSERT INTO inventory_by_product_zone (product_id, zone_id, available_quantity, reserved_quantity, updated_at) VALUES (?, ?, ?, ?, ?)",
		productID, zoneID, available, reserved, now,
	).Exec(); err != nil {
		return err
	}
	if err := r.session.Query(
		"INSERT INTO inventory_by_product (product_id, zone_id, available_quantity, reserved_quantity, updated_at) VALUES (?, ?, ?, ?, ?)",
		productID, zoneID, available, reserved, now,
	).Exec(); err != nil {
		return err
	}
	return r.session.Query(
		"INSERT INTO inventory_by_zone (zone_id, product_id, available_quantity, reserved_quantity, updated_at) VALUES (?, ?, ?, ?, ?)",
		zoneID, productID, available, reserved, now,
	).Exec()
}

type OrderItem struct {
	ProductID string `json:"product_id"`
	ZoneID    string `json:"zone_id"`
	Quantity  int    `json:"quantity"`
}

func (r *Repository) SaveOrder(orderID string, items []OrderItem, createdAt time.Time) error {
	data, err := json.Marshal(items)
	if err != nil {
		return err
	}
	return r.session.Query(
		"INSERT INTO orders (order_id, status, items_json, created_at, completed_at) VALUES (?, ?, ?, ?, ?)",
		orderID, "CREATED", string(data), createdAt, nil,
	).Exec()
}

func (r *Repository) GetOrder(orderID string) (status string, items []OrderItem, found bool, err error) {
	var itemsJSON string
	err = r.session.Query(
		"SELECT status, items_json FROM orders WHERE order_id = ?",
		orderID,
	).Scan(&status, &itemsJSON)
	if err == gocql.ErrNotFound {
		return "", nil, false, nil
	}
	if err != nil {
		return "", nil, false, err
	}
	if itemsJSON != "" {
		if err := json.Unmarshal([]byte(itemsJSON), &items); err != nil {
			return "", nil, false, err
		}
	}
	return status, items, true, nil
}

func (r *Repository) CompleteOrder(orderID string, completedAt time.Time) error {
	return r.session.Query(
		"UPDATE orders SET status = ?, completed_at = ? WHERE order_id = ?",
		"COMPLETED", completedAt, orderID,
	).Exec()
}

func (r *Repository) AppendHistory(ev *events.WarehouseEvent, processedAt time.Time) error {
	eventTS := processedAt
	if ev.Timestamp > 0 {
		eventTS = time.UnixMilli(ev.Timestamp).UTC()
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	qty := events.Int(ev.Quantity)
	return r.session.Query(
		"INSERT INTO event_history (event_id, event_type, product_id, zone_id, quantity, event_timestamp, processed_at, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		ev.EventID,
		ev.EventType,
		events.Str(ev.ProductID),
		events.Str(ev.ZoneID),
		qty,
		eventTS,
		processedAt,
		string(payload),
	).Exec()
}

func WaitReady(hosts []string, keyspace string, retries int) (*Repository, error) {
	var lastErr error
	for i := 1; i <= retries; i++ {
		repo, err := New(hosts, keyspace)
		if err != nil {
			lastErr = err
			time.Sleep(3 * time.Second)
			continue
		}
		if err := repo.Ping(); err != nil {
			repo.Close()
			lastErr = err
			time.Sleep(3 * time.Second)
			continue
		}
		return repo, nil
	}
	return nil, fmt.Errorf("cassandra not ready after %d attempts: %w", retries, lastErr)
}
