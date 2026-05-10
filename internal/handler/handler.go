package handler

import (
	"fmt"
	"log"
	"time"

	"github.com/victor3563/soa6/internal/cassandra"
	"github.com/victor3563/soa6/internal/events"
)

var allowedTypes = map[string]struct{}{
	"PRODUCT_RECEIVED":  {},
	"PRODUCT_SHIPPED":   {},
	"PRODUCT_MOVED":     {},
	"PRODUCT_RESERVED":  {},
	"PRODUCT_RELEASED":  {},
	"INVENTORY_COUNTED": {},
	"ORDER_CREATED":     {},
	"ORDER_COMPLETED":   {},
}

type Handler struct {
	repo *cassandra.Repository
}

func New(repo *cassandra.Repository) *Handler {
	return &Handler{repo: repo}
}

func (h *Handler) Handle(ev *events.WarehouseEvent) error {
	if _, ok := allowedTypes[ev.EventType]; !ok {
		return fmt.Errorf("unknown event_type: %s", ev.EventType)
	}

	var err error
	switch ev.EventType {
	case "PRODUCT_RECEIVED":
		err = h.productReceived(ev)
	case "PRODUCT_SHIPPED":
		err = h.productShipped(ev)
	case "PRODUCT_MOVED":
		err = h.productMoved(ev)
	case "PRODUCT_RESERVED":
		err = h.productReserved(ev)
	case "PRODUCT_RELEASED":
		err = h.productReleased(ev)
	case "INVENTORY_COUNTED":
		err = h.inventoryCounted(ev)
	case "ORDER_CREATED":
		err = h.orderCreated(ev)
	case "ORDER_COMPLETED":
		err = h.orderCompleted(ev)
	}
	if err != nil {
		return err
	}
	return h.repo.AppendHistory(ev, time.Now().UTC())
}

func requirePositive(q int, field string) (int, error) {
	if q <= 0 {
		return 0, fmt.Errorf("invalid %s: %d (must be positive)", field, q)
	}
	return q, nil
}

func requireNonEmpty(values map[string]string) error {
	for name, v := range values {
		if v == "" {
			return fmt.Errorf("missing required field: %s", name)
		}
	}
	return nil
}

func (h *Handler) adjustZone(productID, zoneID string, availableDelta, reservedDelta int) error {
	available, reserved, err := h.repo.GetInventory(productID, zoneID)
	if err != nil {
		return err
	}
	available += availableDelta
	reserved += reservedDelta
	if available < 0 || reserved < 0 {
		return fmt.Errorf("negative inventory for %s@%s: available=%d reserved=%d", productID, zoneID, available, reserved)
	}
	return h.repo.WriteInventory(productID, zoneID, available, reserved)
}

func (h *Handler) productReceived(ev *events.WarehouseEvent) error {
	if err := requireNonEmpty(map[string]string{
		"product_id": events.Str(ev.ProductID),
		"zone_id":    events.Str(ev.ZoneID),
	}); err != nil {
		return err
	}
	qty, err := requirePositive(events.Int(ev.Quantity), "quantity")
	if err != nil {
		return err
	}
	return h.adjustZone(events.Str(ev.ProductID), events.Str(ev.ZoneID), qty, 0)
}

func (h *Handler) productShipped(ev *events.WarehouseEvent) error {
	if err := requireNonEmpty(map[string]string{
		"product_id": events.Str(ev.ProductID),
		"zone_id":    events.Str(ev.ZoneID),
	}); err != nil {
		return err
	}
	qty, err := requirePositive(events.Int(ev.Quantity), "quantity")
	if err != nil {
		return err
	}
	return h.adjustZone(events.Str(ev.ProductID), events.Str(ev.ZoneID), -qty, 0)
}

func (h *Handler) productMoved(ev *events.WarehouseEvent) error {
	if err := requireNonEmpty(map[string]string{
		"product_id":   events.Str(ev.ProductID),
		"from_zone_id": events.Str(ev.FromZoneID),
		"to_zone_id":   events.Str(ev.ToZoneID),
	}); err != nil {
		return err
	}
	qty, err := requirePositive(events.Int(ev.Quantity), "quantity")
	if err != nil {
		return err
	}
	if err := h.adjustZone(events.Str(ev.ProductID), events.Str(ev.FromZoneID), -qty, 0); err != nil {
		return err
	}
	return h.adjustZone(events.Str(ev.ProductID), events.Str(ev.ToZoneID), qty, 0)
}

func (h *Handler) productReserved(ev *events.WarehouseEvent) error {
	if err := requireNonEmpty(map[string]string{
		"product_id": events.Str(ev.ProductID),
		"zone_id":    events.Str(ev.ZoneID),
	}); err != nil {
		return err
	}
	qty, err := requirePositive(events.Int(ev.Quantity), "quantity")
	if err != nil {
		return err
	}
	return h.adjustZone(events.Str(ev.ProductID), events.Str(ev.ZoneID), -qty, qty)
}

func (h *Handler) productReleased(ev *events.WarehouseEvent) error {
	if err := requireNonEmpty(map[string]string{
		"product_id": events.Str(ev.ProductID),
		"zone_id":    events.Str(ev.ZoneID),
	}); err != nil {
		return err
	}
	qty, err := requirePositive(events.Int(ev.Quantity), "quantity")
	if err != nil {
		return err
	}
	return h.adjustZone(events.Str(ev.ProductID), events.Str(ev.ZoneID), qty, -qty)
}

func (h *Handler) inventoryCounted(ev *events.WarehouseEvent) error {
	if err := requireNonEmpty(map[string]string{
		"product_id": events.Str(ev.ProductID),
		"zone_id":    events.Str(ev.ZoneID),
	}); err != nil {
		return err
	}
	counted := events.Int(ev.CountedQuantity)
	if counted < 0 {
		return fmt.Errorf("invalid counted_quantity: %d", counted)
	}
	_, reserved, err := h.repo.GetInventory(events.Str(ev.ProductID), events.Str(ev.ZoneID))
	if err != nil {
		return err
	}
	return h.repo.WriteInventory(events.Str(ev.ProductID), events.Str(ev.ZoneID), counted, reserved)
}

func (h *Handler) orderCreated(ev *events.WarehouseEvent) error {
	orderID := events.Str(ev.OrderID)
	if orderID == "" {
		return fmt.Errorf("missing order_id")
	}
	if len(ev.OrderItems) == 0 {
		return fmt.Errorf("ORDER_CREATED requires order_items")
	}

	_, _, found, err := h.repo.GetOrder(orderID)
	if err != nil {
		return err
	}
	if found {
		log.Printf("WARN order %s already exists, skipping duplicate create", orderID)
		return nil
	}

	items := make([]cassandra.OrderItem, len(ev.OrderItems))
	for i, it := range ev.OrderItems {
		items[i] = cassandra.OrderItem{
			ProductID: it.ProductID,
			ZoneID:    it.ZoneID,
			Quantity:  it.Quantity,
		}
	}
	if err := h.repo.SaveOrder(orderID, items, time.Now().UTC()); err != nil {
		return err
	}
	for _, it := range items {
		qty, err := requirePositive(it.Quantity, "quantity")
		if err != nil {
			return err
		}
		if err := h.adjustZone(it.ProductID, it.ZoneID, -qty, qty); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) orderCompleted(ev *events.WarehouseEvent) error {
	orderID := events.Str(ev.OrderID)
	if orderID == "" {
		return fmt.Errorf("missing order_id")
	}

	status, items, found, err := h.repo.GetOrder(orderID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("order not found: %s", orderID)
	}
	if status == "COMPLETED" {
		log.Printf("WARN order %s already completed", orderID)
		return nil
	}

	for _, it := range items {
		qty, err := requirePositive(it.Quantity, "quantity")
		if err != nil {
			return err
		}
		available, reserved, err := h.repo.GetInventory(it.ProductID, it.ZoneID)
		if err != nil {
			return err
		}
		reserved -= qty
		if reserved < 0 {
			return fmt.Errorf("cannot complete order: reserved would be negative for %s@%s", it.ProductID, it.ZoneID)
		}
		if err := h.repo.WriteInventory(it.ProductID, it.ZoneID, available, reserved); err != nil {
			return err
		}
	}
	return h.repo.CompleteOrder(orderID, time.Now().UTC())
}
