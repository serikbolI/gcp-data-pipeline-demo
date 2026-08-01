-- Replace demo-analytics-project with your GCP project ID.
INSERT INTO `demo-analytics-project.raw.orders`
  (event_id, order_id, customer_id, event_type, event_ts, amount, currency, ingested_at)
VALUES
  ('evt-1001', 'order-101', 'customer-1', 'CREATED',   TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), NULL,    'USD', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)),
  ('evt-1002', 'order-101', 'customer-1', 'PAID',      TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), 125.50,  'USD', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)),
  ('evt-1003', 'order-102', 'customer-2', 'CREATED',   TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR), NULL,    'USD', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)),
  ('evt-1004', 'order-102', 'customer-2', 'CANCELLED', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE), NULL, 'USD', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)),
  -- Duplicate source event. The later ingested record should win.
  ('evt-1002', 'order-101', 'customer-1', 'PAID',      TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), 125.50,  'USD', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)),
  -- Late event: old business time, recent ingestion time.
  ('evt-1005', 'order-103', 'customer-3', 'PAID',      TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY), 80.00,   'EUR', CURRENT_TIMESTAMP());
