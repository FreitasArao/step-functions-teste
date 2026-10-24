# Order Saga — AWS Step Functions + SQS (`waitForTaskToken`) + Outbox/Inbox + DDD + Result Pattern + `dd-trace`

Laboratório prático (**build to learn**) demonstrando como orquestrar uma **Saga com Rollback em Cascata (2 níveis)** no **AWS Step Functions** (via **MiniStack** e **Terraform**) mantendo a lógica de negócio 100% desacoplada da AWS através de:

- **DDD & Monolito Modular** (`Order`, `Stock`, `Payment`, `Shipping`) com **Ports & Adapters**
- **Result Pattern (`Result<T, E>`)** em todo o domínio e casos de uso (**zero exceptions** para regras de negócio)
- **AWS SQS (`sendMessage.waitForTaskToken`)** para execução assíncrona dos steps
- **Transactional Outbox & Transactional Inbox** em todo o fluxo (garantia de entrega + idempotência contra entregas duplicadas do SQS)
- **`MessageWrapper<T>` + Datadog `dd-trace`** para propagação de **Trace Distribuído** (`x-datadog-trace-id`, `x-datadog-parent-id`, `traceparent`) através de banco (Outbox/Inbox), Step Functions e SQS

---

## 1. Topologia da Saga e Rollback em Cascata

```text
[POST /orders]
   │ (UnitOfWork: salva Order PENDING + Outbox START_ORDER_SAGA com MessageWrapper)
   ▼
[Step 1: RESERVE_STOCK] ──(Erro: INSUFFICIENT_STOCK)──────────────────────────────┐
   │ (Ok)                                                                         │
   ▼                                                                              │
[Step 2: PROCESS_PAYMENT] ──(Erro: PAYMENT_DECLINED)──► [RELEASE_STOCK] ──────────┤
   │ (Ok)                                                      ▲                  │
   ▼                                                           │                  ▼
[Step 3: CREATE_SHIPMENT] ──(Erro: SHIPPING_UNAVAILABLE)► [REFUND_PAYMENT]   [CANCEL_ORDER]
   │ (Ok)                                                                         │
   ▼                                                                              ▼
[Step 4: CONFIRM_ORDER] ──► (OrderConfirmed: SUCCEEDED)                (OrderCancelled: FAILED)
```

### Regras de Domínio Determinísticas (para acionar cada cenário via payload)
- **Estoque (`StockItem`):** `SKU-100` inicia com **10 unidades disponíveis**. Se `quantity > 10`, retorna `Result.err(InsufficientStockError)` (`code: "INSUFFICIENT_STOCK"`).
- **Pagamento (`Payment`):** Limite máximo aprovado é **`1000`**. Se `amount > 1000`, retorna `Result.err(PaymentDeclinedError)` (`code: "PAYMENT_DECLINED"`).
- **Envio (`Shipment`):** CEPs iniciados com **`"99"`** (ex: `"99000-000"`) não possuem cobertura de entrega e retornam `Result.err(ShippingUnavailableError)` (`code: "SHIPPING_UNAVAILABLE"`).

---

## 2. Subindo o Ambiente Passo a Passo

### Passo 2.1 — Instalar dependências e rodar os testes automatizados
```bash
bun install
bun run test
```

### Passo 2.2 — Subir o PostgreSQL 16 + MiniStack (Docker) e provisionar Step Functions + SQS com Terraform
```bash
# 1. Subir os containers do PostgreSQL (porta 5432) e do MiniStack (porta 4566)
docker compose up -d

# 2. Provisionar a fila SQS, DLQ, IAM Role e State Machine no MiniStack
cd infra
terraform init
terraform apply -auto-approve
cd ..
```

> **Nota:** O container `step-function-postgres` já executa automaticamente o script [`infra/postgres/init.sql`](./infra/postgres/init.sql) criando as 7 tabelas (`orders`, `stock_items`, `stock_reservations`, `payments`, `shipments`, `inbox_messages`, `outbox_messages`) e populando o estoque inicial (`SKU-100` com 10 unidades). Caso você rode sem o Docker, a aplicação faz fallback transparente para memória e para a engine ASL local (`AslLocalEngineService`).

### Passo 2.3 — Iniciar a aplicação NestJS
```bash
bun run start:dev
```
A API estará disponível em `http://localhost:3000`.

---

## 3. Como Testar Cada Etapa e os Rollbacks

Antes de começar (ou entre um teste e outro), você pode conferir o estoque inicial ou resetar o banco:

```bash
# Ver saldo atual do estoque (SKU-100 começa com availableQuantity = 10, reservedQuantity = 0)
curl -s http://localhost:3000/stock | jq

# Resetar todas as tabelas do banco para o estado inicial a qualquer momento
curl -s -X POST http://localhost:3000/db/reset | jq
```

---

### Cenário 1: Caminho Feliz (`ReserveStock` ➔ `ProcessPayment` ➔ `CreateShipment` ➔ `ConfirmOrder`)

**1. Criar o pedido:**
```bash
curl -s -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -H "x-datadog-trace-id: 1000000000000000001" \
  -H "x-datadog-parent-id: 2000000000000000001" \
  -d '{
    "sku": "SKU-100",
    "quantity": 2,
    "amount": 450,
    "zipCode": "01001-000"
  }' | jq
```

**2. Validar o estado do pedido, Inbox, Outbox e Trace Distribuído (substitua `<ORDER_ID>` pelo `order.id` retornado):**
```bash
curl -s http://localhost:3000/orders/<ORDER_ID> | jq
```
**O que validar:**
- `order.status`: `"CONFIRMED"`
- `domainState.stockReservation.status`: `"RESERVED"`
- `domainState.currentSkuStock`: `availableQuantity: 8`, `reservedQuantity: 2`
- `domainState.payment.status`: `"APPROVED"`
- `domainState.shipment.status`: `"DISPATCHED"`
- `transactionalMessaging.inboxProcessedSteps`: 4 steps processados (`RESERVE_STOCK`, `PROCESS_PAYMENT`, `CREATE_SHIPMENT`, `CONFIRM_ORDER`)
- `distributedTracing.traceIds`: `["1000000000000000001"]` (o mesmo `traceId` em todos os spans e envelopes `MessageWrapper`!)

---

### Cenário 2: Falha no Passo 1 — Estoque Insuficiente (`ReserveStock [ERR]` ➔ `CancelOrder`)

Solicitamos `quantity: 50` (maior que o saldo disponível):

```bash
curl -s -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "sku": "SKU-100",
    "quantity": 50,
    "amount": 450,
    "zipCode": "01001-000"
  }' | jq
```

**Validar o resultado (`GET /orders/<ORDER_ID>` e `GET /stock`):**
- `order.status`: `"CANCELLED"`
- `order.cancellationReason`: `"INSUFFICIENT_STOCK: Insufficient stock for SKU 'SKU-100'..."`
- `transactionalMessaging.inboxProcessedSteps`: apenas `RESERVE_STOCK` (`ERR`) e `CANCEL_ORDER` (`OK`).
- O saldo do `SKU-100` em `GET /stock` permanece intacto.

---

### Cenário 3: Rollback de 1 Nível — Pagamento Recusado (`ReserveStock [OK]` ➔ `ProcessPayment [ERR]` ➔ `ReleaseStock [ROLLBACK]` ➔ `CancelOrder`)

Solicitamos `quantity: 3` (que reserva 3 unidades no estoque) e `amount: 2500` (acima do limite de `1000`, causando recusa no pagamento):

```bash
curl -s -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "sku": "SKU-100",
    "quantity": 3,
    "amount": 2500,
    "zipCode": "01001-000"
  }' | jq
```

**Validar o rollback de 1 nível (`GET /orders/<ORDER_ID>`):**
- `order.status`: `"CANCELLED"`
- `order.cancellationReason`: `"PAYMENT_DECLINED: Payment of 2500 for order ... was declined"`
- `domainState.payment.status`: `"DECLINED"`
- `domainState.stockReservation.status`: **`"RELEASED"`** (a reserva criada no Passo 1 foi estornada!)
- `domainState.currentSkuStock`: as 3 unidades voltaram para `availableQuantity`!
- `transactionalMessaging.inboxProcessedSteps`:
  1. `RESERVE_STOCK` (`OK`)
  2. `PROCESS_PAYMENT` (`ERR`)
  3. `RELEASE_STOCK` (`OK`) — **Compensação Nível 1**
  4. `CANCEL_ORDER` (`OK`)

---

### Cenário 4: Rollback em Cascata de 2 Níveis — Falha na Logística (`ReserveStock [OK]` ➔ `ProcessPayment [OK]` ➔ `CreateShipment [ERR]` ➔ `RefundPayment [ROLLBACK 2]` ➔ `ReleaseStock [ROLLBACK 1]` ➔ `CancelOrder`)

Solicitamos `quantity: 4`, `amount: 800` (estoque e pagamento aprovados!) e `zipCode: "99000-000"` (CEP sem cobertura de entrega):

```bash
curl -s -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "sku": "SKU-100",
    "quantity": 4,
    "amount": 800,
    "zipCode": "99000-000"
  }' | jq
```

**Validar o rollback em cascata (`GET /orders/<ORDER_ID>`):**
- `order.status`: `"CANCELLED"`
- `order.cancellationReason`: `"SHIPPING_UNAVAILABLE: Shipping unavailable..."`
- `domainState.shipment.status`: `"FAILED"`
- `domainState.payment.status`: **`"REFUNDED"`** (o pagamento que havia sido `APPROVED` no Passo 2 foi estornado pelo `RefundPayment`!)
- `domainState.stockReservation.status`: **`"RELEASED"`** (a reserva de 4 unidades feita no Passo 1 foi liberada em cascata pelo `ReleaseStock`!)
- `transactionalMessaging.inboxProcessedSteps`:
  1. `RESERVE_STOCK` (`OK`)
  2. `PROCESS_PAYMENT` (`OK`)
  3. `CREATE_SHIPMENT` (`ERR`)
  4. `REFUND_PAYMENT` (`OK`) — **Compensação Nível 2**
  5. `RELEASE_STOCK` (`OK`) — **Compensação Nível 1 (em cascata)**
  6. `CANCEL_ORDER` (`OK`)

---

## 4. Como Inspecionar o Banco de Dados PostgreSQL (`psql` e `GET /db`)

### 4.1 Consultando diretamente no PostgreSQL via `psql`

Com o `docker compose up -d` rodando, você pode inspecionar as tabelas reais do **Transactional Outbox**, **Transactional Inbox** e de cada domínio direto no PostgreSQL:

```bash
# 1. Ver eventos da Transactional Outbox (START_ORDER_SAGA, SFN_TASK_SUCCESS, SFN_TASK_FAILURE)
docker exec -it step-function-postgres psql -U saga_user -d order_saga_db -c \
  "SELECT id, order_id, event_type, status, created_at, published_at FROM outbox_messages ORDER BY created_at;"

# 2. Ver chaves de idempotência na Transactional Inbox (PRIMARY KEY order_id:step_name)
docker exec -it step-function-postgres psql -U saga_user -d order_saga_db -c \
  "SELECT idempotency_key, step_name, outcome, processed_at, wrapper->'metadata'->'traceContext' AS trace_ctx FROM inbox_messages ORDER BY processed_at;"

# 3. Ver saldo disponível vs reservado no Estoque e status das Reservas
docker exec -it step-function-postgres psql -U saga_user -d order_saga_db -c \
  "SELECT * FROM stock_items; SELECT id, order_id, sku, quantity, status, released_at FROM stock_reservations;"

# 4. Ver Pedidos, Pagamentos (APPROVED / DECLINED / REFUNDED) e Envios (DISPATCHED / FAILED)
docker exec -it step-function-postgres psql -U saga_user -d order_saga_db -c \
  "SELECT id, sku, quantity, amount, zip_code, status, cancellation_reason FROM orders; SELECT id, order_id, amount, status, refunded_at FROM payments; SELECT id, order_id, zip_code, status, tracking_code FROM shipments;"
```

### 4.2 Testando o Transactional Outbox "Pausado" (`?wait=false`)
Se você quiser ver a mensagem gravada como `PENDING` na tabela `outbox_messages` no mesmo milissegundo em que o `POST /orders` faz `COMMIT` (antes do worker publicar):
```bash
DISABLE_BACKGROUND_WORKERS=true bun run start:dev
# Em outro terminal:
curl -s -X POST "http://localhost:3000/orders?wait=false" \
  -H "Content-Type: application/json" \
  -d '{"sku": "SKU-100", "quantity": 2, "amount": 450, "zipCode": "01001-000"}' | jq

# Consulte no Postgres: você verá Order = PENDING e Outbox = PENDING na mesma transação ACID!
docker exec -it step-function-postgres psql -U saga_user -d order_saga_db -c \
  "SELECT id, order_id, event_type, status FROM outbox_messages;"
```

### 4.3 Consultando via Endpoint HTTP (`GET /db`)

Você também pode inspecionar todas as tabelas (e qual engine está ativa em `databaseEngine: "postgresql" | "in-memory"`) via HTTP:

```bash
# Dump completo de todas as tabelas
curl -s http://localhost:3000/db | jq

# Consultar apenas a tabela de Pedidos (orders)
curl -s http://localhost:3000/db | jq '.tables.orders'

# Consultar saldo de estoque e histórico de reservas (stock_items e stock_reservations)
curl -s http://localhost:3000/db | jq '.tables.stock_items, .tables.stock_reservations'

# Consultar tabela de Pagamentos (payments) e Envios (shipments)
curl -s http://localhost:3000/db | jq '.tables.payments, .tables.shipments'

# Consultar a Transactional Inbox (chaves de idempotência orderId:stepName + MessageWrapper)
curl -s http://localhost:3000/db | jq '.tables.inbox_messages'

# Consultar a Transactional Outbox (START_ORDER_SAGA, SFN_TASK_SUCCESS, SFN_TASK_FAILURE)
curl -s http://localhost:3000/db | jq '.tables.outbox_messages'

# Consultar todos os Spans do Datadog (traceId, spanId, parentSpanId, operationName, tags)
curl -s http://localhost:3000/db | jq '.tables.telemetry_spans'
```
