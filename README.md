# Kafka Inventory Management System

A Spring Boot inventory management application demonstrating event-driven inventory updates using Apache Kafka and PostgreSQL.

The application supports standard REST operations for managing inventory while also providing asynchronous inventory updates through Kafka.

## Features

- Create inventory for a product
- Retrieve inventory by product ID
- Retrieve all inventory records
- Set an exact inventory quantity using PUT
- Increment or decrement inventory using PATCH
- Delete inventory records
- Publish inventory events asynchronously through Kafka
- Consume Kafka events and update inventory in PostgreSQL
- Prevent inventory quantity from becoming negative
- Custom exception handling
- Request validation
- Transaction management for database updates

## Technology Stack

- Java
- Spring Boot
- Spring MVC
- Spring Data JPA
- Apache Kafka 3.9.2
- PostgreSQL
- Hibernate
- Jakarta Bean Validation
- Maven
- Docker

## Architecture

The application uses a layered architecture for synchronous REST operations:

```text
Client
  |
  v
InventoryController
  |
  v
InventoryService
  |
  v
InventoryRepository
  |
  v
PostgreSQL
```

Kafka-based inventory updates follow an asynchronous event-driven flow:

```text
Client
  |
  v
POST /inventory/event
  |
  v
InventoryProducer
  |
  v
Kafka Topic: inventory-events
  |
  v
InventoryConsumer
  |
  v
InventoryRepository
  |
  v
PostgreSQL
```

## Kafka Configuration

Kafka runs locally on:

```text
localhost:9092
```

The application uses:

```text
Topic: inventory-events
Consumer Group: inventory-events
```

The Kafka topic and consumer group are defined in `KafkaConstants`.

The Kafka broker can be configured in `application.properties`:

```properties
spring.kafka.bootstrap-servers=localhost:9092
```

## PostgreSQL Configuration

The application uses PostgreSQL for persistent inventory storage.

Example configuration:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/inventory
spring.datasource.username=admin
spring.datasource.password=admin

spring.jpa.hibernate.ddl-auto=update
spring.jpa.show-sql=true
```

For a real production environment, database credentials should be supplied through environment variables or a secrets-management system rather than committed to source control.

## Running Kafka with Docker

Kafka can be started locally using Docker:

```bash
docker run -d \
  --name kafka \
  -p 9092:9092 \
  apache/kafka:3.9.2
```

Verify that the container is running:

```bash
docker ps
```

Kafka should be available on:

```text
localhost:9092
```

## REST APIs

### Create Inventory

```http
POST /inventory
```

Request:

```json
{
  "productId": "PRODUCT-1001",
  "quantity": 100
}
```

Example response:

```json
{
  "id": 1,
  "productId": "PRODUCT-1001",
  "quantity": 100
}
```

Returns:

```text
201 Created
```

---

### Get All Inventory

```http
GET /inventory
```

Example response:

```json
[
  {
    "id": 1,
    "productId": "PRODUCT-1001",
    "quantity": 100
  }
]
```

---

### Get Inventory by Product ID

```http
GET /inventory/PRODUCT-1001
```

Example response:

```json
{
  "id": 1,
  "productId": "PRODUCT-1001",
  "quantity": 100
}
```

---

### Replace Inventory Quantity

PUT sets the inventory quantity to an exact value.

```http
PUT /inventory/PRODUCT-1001
```

Request:

```json
{
  "quantity": 50
}
```

If the current quantity is `100`, the new quantity becomes exactly `50`.

Example response:

```json
{
  "id": 1,
  "productId": "PRODUCT-1001",
  "quantity": 50
}
```

Negative quantities are rejected through request validation.

---

### Adjust Inventory Quantity

PATCH increments or decrements the existing quantity.

```http
PATCH /inventory/PRODUCT-1001
```

Request:

```json
{
  "delta": 20
}
```

If the current quantity is `50`, the new quantity becomes `70`.

Example response:

```json
{
  "status": "updated",
  "productId": "PRODUCT-1001",
  "delta": 20,
  "quantity": 70
}
```

Inventory can also be decreased:

```json
{
  "delta": -10
}
```

The application prevents an adjustment from reducing the inventory below zero.

---

### Delete Inventory

```http
DELETE /inventory/1
```

Returns:

```text
204 No Content
```

## Kafka Inventory Event

Inventory can also be updated asynchronously by publishing an event to Kafka.

```http
POST /inventory/event
```

Request:

```json
{
  "productId": "PRODUCT-1001",
  "delta": -10
}
```

If `eventId` is not supplied, the application generates a UUID and timestamp before publishing the event.

The endpoint returns:

```text
202 Accepted
```

The event is published to:

```text
inventory-events
```

The consumer receives the event and updates PostgreSQL.

For example:

```text
Current quantity: 70
Kafka delta:      -10
Final quantity:    60
```

The result can then be verified using:

```http
GET /inventory/PRODUCT-1001
```

Example:

```json
{
  "id": 1,
  "productId": "PRODUCT-1001",
  "quantity": 60
}
```

## Kafka Message Key

The producer publishes the product ID as the Kafka message key.

Conceptually:

```text
Key   = PRODUCT-1001
Value = InventoryEvent JSON
```

Using `productId` as the key helps Kafka route events for the same product consistently to the same partition.

## Transaction Management

Database-modifying service operations use Spring's `@Transactional`.

Transactional operations include:

- Replacing inventory quantity
- Adjusting inventory quantity
- Creating inventory
- Deleting inventory

This ensures database operations are executed within transactional boundaries.

## Validation

Inventory creation and replacement requests use Jakarta Bean Validation.

Inventory quantities cannot be negative.

The PATCH operation additionally checks the current inventory before applying a delta.

For example:

```text
Current quantity = 5
Delta            = -10
Result           = -5
```

The operation is rejected because inventory cannot fall below zero.

## Exception Handling

The application uses custom exceptions for business errors.

### InventoryNotFoundException

Used when the requested inventory record does not exist.

Example:

```text
404 Not Found
```

### InsufficientInventoryException

Used when an inventory adjustment would reduce quantity below zero.

Example:

```text
409 Conflict
```

A global exception handler converts application exceptions into appropriate HTTP responses.

## Project Structure

```text
src/main/java/com/kafka/kafkaInventory
|
+-- constants
|   +-- KafkaConstants
|
+-- controller
|   +-- InventoryController
|
+-- dto
|   +-- InventoryCreateRequest
|   +-- InventoryEvent
|   +-- InventoryReplaceRequest
|   +-- InventoryResponse
|   +-- InventoryUpdateRequest
|   +-- InventoryUpdateResponse
|
+-- exception
|   +-- InventoryNotFoundException
|   +-- InsufficientInventoryException
|   +-- ApiErrorResponse
|   +-- GlobalExceptionHandler
|
+-- model
|   +-- Inventory
|   +-- InventoryConsumer
|
+-- repository
|   +-- InventoryRepository
|
+-- service
    +-- InventoryProducer
    +-- InventoryService
```

## Running the Application

### 1. Start PostgreSQL

Ensure PostgreSQL is running and the `inventory` database is available.

### 2. Start Kafka

```bash
docker start kafka
```

If the Kafka container does not exist, create it using:

```bash
docker run -d \
  --name kafka \
  -p 9092:9092 \
  apache/kafka:3.9.2
```

### 3. Run the Spring Boot application

```bash
./mvnw spring-boot:run
```

or:

```bash
mvn spring-boot:run
```

### 4. Verify the application

Create inventory:

```http
POST http://localhost:8080/inventory
```

Then retrieve it:

```http
GET http://localhost:8080/inventory
```

## Example End-to-End Kafka Flow

Create initial inventory:

```text
PRODUCT-1001 = 100
```

Replace quantity using PUT:

```text
100 -> 50
```

Adjust quantity using PATCH:

```text
50 + 20 -> 70
```

Publish Kafka event:

```text
delta = -10
```

Kafka consumer processes the event:

```text
70 - 10 -> 60
```

Final PostgreSQL state:

```text
PRODUCT-1001 = 60
```

This demonstrates both synchronous REST-based inventory management and asynchronous Kafka-based inventory processing.