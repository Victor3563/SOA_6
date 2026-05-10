package events

// WarehouseEvent соответствует Avro-схеме warehouse_event.avsc.
type WarehouseEvent struct {
	EventID         string      `avro:"event_id"`
	EventType       string      `avro:"event_type"`
	Timestamp       int64       `avro:"timestamp"`
	ProductID       *string     `avro:"product_id"`
	ZoneID          *string     `avro:"zone_id"`
	FromZoneID      *string     `avro:"from_zone_id"`
	ToZoneID        *string     `avro:"to_zone_id"`
	Quantity        *int        `avro:"quantity"`
	CountedQuantity *int        `avro:"counted_quantity"`
	OrderID         *string     `avro:"order_id"`
	OrderItems      []OrderItem `avro:"order_items"`
}

type OrderItem struct {
	ProductID string `avro:"product_id" json:"product_id"`
	ZoneID    string `avro:"zone_id" json:"zone_id"`
	Quantity  int    `avro:"quantity" json:"quantity"`
}

func Str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func Int(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}

func StrPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func IntPtr(v int) *int {
	return &v
}
