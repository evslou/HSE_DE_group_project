CREATE TABLE users
(
  user_id INT NOT NULL,
  user_phone VARCHAR NOT NULL,
  PRIMARY KEY (user_id)
);

CREATE TABLE driver
(
  driver_id INT NOT NULL,
  driver_phone VARCHAR NOT NULL,
  PRIMARY KEY (driver_id)
);

CREATE TABLE store
(
  store_id INT NOT NULL,
  store_address VARCHAR NOT NULL,
  PRIMARY KEY (store_id)
);

CREATE TABLE payment_type
(
  payment_type_id INT NOT NULL,
  payment_type VARCHAR NOT NULL,
  PRIMARY KEY (payment_type_id)
);

CREATE TABLE item_category
(
  item_category_id INT NOT NULL,
  item_category VARCHAR NOT NULL,
  PRIMARY KEY (item_category_id)
);

CREATE TABLE orders
(
  order_id INT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  paid_at TIMESTAMP,
  canceled_at TIMESTAMP,
  order_discount FLOAT NOT NULL,
  order_cancellation_reason VARCHAR,
  user_id INT NOT NULL,
  store_id INT NOT NULL,
  payment_type_id INT NOT NULL,
  PRIMARY KEY (order_id),
  FOREIGN KEY (user_id) REFERENCES users(user_id),
  FOREIGN KEY (store_id) REFERENCES store(store_id),
  FOREIGN KEY (payment_type_id) REFERENCES payment_type(payment_type_id)
);

CREATE TABLE items
(
  item_id INT NOT NULL,
  item_title VARCHAR NOT NULL,
  item_price FLOAT NOT NULL,
  validity_datetime_start TIMESTAMP NOT NULL,
  validity_datetime_end TIMESTAMP NOT NULL,
  item_category_id INT NOT NULL,
  PRIMARY KEY (item_id, validity_datetime_start, validity_datetime_end),
  FOREIGN KEY (item_category_id) REFERENCES item_category(item_category_id)
);

CREATE TABLE order_to_item
(
  item_quantity FLOAT NOT NULL,
  item_canceled_quantity FLOAT NOT NULL,
  item_replaced_id INT NOT NULL,
  item_discount FLOAT NOT NULL,
  order_id INT NOT NULL,
  item_id INT NOT NULL,
  validity_datetime_start TIMESTAMP NOT NULL,
  validity_datetime_end TIMESTAMP NOT NULL,
  PRIMARY KEY (order_id, item_id, validity_datetime_start, validity_datetime_end),
  FOREIGN KEY (order_id) REFERENCES orders(order_id),
  FOREIGN KEY (item_id, validity_datetime_start, validity_datetime_end) REFERENCES items(item_id, validity_datetime_start, validity_datetime_end)
);

CREATE TABLE delivery
(
  delivery_cost FLOAT NOT NULL,
  delivery_started_at TIMESTAMP,
  delivered_at TIMESTAMP,
  address_text VARCHAR NOT NULL,
  driver_id INT NOT NULL,
  order_id INT NOT NULL,
  PRIMARY KEY (driver_id, order_id),
  FOREIGN KEY (driver_id) REFERENCES driver(driver_id),
  FOREIGN KEY (order_id) REFERENCES orders(order_id)
);