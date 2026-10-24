# Order Saga — AWS Step Functions + SQS (`waitForTaskToken`) + Outbox/Inbox + DDD + Result Pattern + `dd-trace`

Laboratório prático (**build to learn**) demonstrando como orquestrar uma **Saga com Rollback em Cascata (2 níveis)** no **AWS Step Functions** (via **MiniStack**, **PostgreSQL 16** e **Terraform**) mantendo a lógica de negócio **100% agnóstica da AWS** através de:

- **DDD & Monolito Modular** (`Order`, `Stock`, `Payment`, `Shipping`) com **Ports & Adapters**
- **Camada de Tradução / Anti-Corruption Layer AWS (`AwsStepFunctionsTranslator`)** isolando `taskToken`, `executionArn` e formato de erro ASL fora do domínio
- **Result Pattern (`Result<T, E>`)** em todo o domínio e casos de uso (**zero exceptions** para regras de negócio)
- **AWS SQS (`sendMessage.waitForTaskToken`)** para execução assíncrona dos steps
- **Transactional Outbox & Transactional Inbox** no PostgreSQL 16 (garantia de entrega + idempotência contra entregas duplicadas do SQS)
- **`MessageWrapper<T>` + Datadog `dd-trace`** para propagação de **Trace Distribuído** (`x-datadog-trace-id`, `x-datadog-parent-id`, `traceparent`) através de banco (Outbox/Inbox), Step Functions e SQS

---

## Como o Fluxo Funciona ao Bater em `POST /orders`?

**Sim!** Uma vez com o ambiente de pé (na branch `master`, ou após você completar os `TODO`s na branch `practice/saga`), **basta fazer um `POST /orders`** e todo o ciclo abaixo acontece automaticamente em background:

```text
1. [HTTP POST /orders]
     │
     ▼
2. [CreateOrderUseCase + PostgresUnitOfWork]
     ├── Salva Order (status = 'PENDING') na tabela `orders`
     └── Salva evento 'ORDER_WORKFLOW_REQUESTED' (status = 'PENDING') na tabela `outbox_messages`
     │
     ▼ (Polling automático a cada 250ms pelo OutboxRelayWorker -> RelayOutboxUseCase)
3. [StepFunctionsOrderWorkflowAdapter + AwsStepFunctionsTranslator]
     ├── Traduz MessageWrapper para StartExecutionCommand do AWS Step Functions
     ├── Inicia a State Machine no MiniStack (usando orderId como nome da execução)
     └── Marca o registro da Outbox como 'PUBLISHED'
     │
     ▼
4. [MiniStack Step Functions -> MiniStack SQS]
     ├── A State Machine entra no passo 1 ('ReserveStock')
     ├── Publica mensagem JSON na fila SQS `order-saga-tasks-queue` com `$$.Task.Token`
     └── Pausa a execução esperando callback (.waitForTaskToken)
     │
     ▼ (Polling automático a cada 300ms pelo SqsSagaStepWorker)
5. [AwsStepFunctionsTranslator -> ExecuteSagaStepUseCase]
     ├── Traduz o JSON da AWS (taskToken) para `WorkflowStepCommand` agnóstico (correlationToken)
     ├── Abre transação ACID no Postgres (`PostgresUnitOfWork`):
     │     1. Verifica idempotência na tabela `inbox_messages` (`orderId:stepName`)
     │     2. Executa o Use Case de Domínio puro (`Result<T, DomainError>`)
     │     3. Salva na `inbox_messages` (`outcome = 'OK'` ou `'ERR'`)
     │     4. Salva na `outbox_messages` (`WORKFLOW_STEP_COMPLETED` ou `WORKFLOW_STEP_FAILED`)
     └── Deleta a mensagem da fila SQS
     │
     ▼ (Polling automático pelo OutboxRelayWorker)
6. [Callback para o Step Functions]
     ├── Se 'WORKFLOW_STEP_COMPLETED' -> envia `SendTaskSuccessCommand` -> avança para o próximo Step!
     └── Se 'WORKFLOW_STEP_FAILED'    -> envia `SendTaskFailureCommand` -> aciona o `Catch` (Rollback em Cascata)!
```

> **Atenção sobre as Branches:**
> - **Branch `master`:** Possui toda a implementação pronta e funcionando ponta a ponta.
> - **Branch `practice/saga`:** Possui o Banco de Dados (Postgres), Entidades de Domínio, Repositórios e Use Cases de Domínio prontos, deixando os **6 arquivos da Saga/AWS com `TODO (Prática)`** para você implementar!

---

## 1. Passo a Passo: Como Rodar Tudo com MiniStack + PostgreSQL + Terraform

### Passo 1.1 — Subir o PostgreSQL 16 e o MiniStack via Docker Compose
Na raiz do projeto, execute:
```bash
docker compose up -d
```
Verifique se os 2 containers estão rodando (`step-function-postgres` na porta `5432` e `step-function-ministack` na porta `4566`):
```bash
docker compose ps
```

### Passo 1.2 — Criar a Fila SQS e a State Machine no MiniStack via Terraform
```bash
cd infra
terraform init
terraform apply -auto-approve
cd ..
```
Ao finalizar, o Terraform exibirá os outputs:
- `sqs_queue_url` (`http://localhost:4566/000000000000/order-saga-tasks-queue`)
- `state_machine_arn` (`arn:aws:states:us-east-1:000000000000:stateMachine:order-saga-state-machine`)

### Passo 1.3 — Iniciar a Aplicação NestJS
```bash
bun run start:dev
```
A aplicação subirá em `http://localhost:3000`, conectará automaticamente ao PostgreSQL (`order_saga_db`) e iniciará os workers de background (`OutboxRelayWorker` e `SqsSagaStepWorker`).

---

## 2. Roteiro de Implementação na Branch `practice/saga`

Se você estiver na branch `practice/saga`, implemente os `TODO`s nesta sequência:

1. [`src/modules/order/application/create-order.use-case.ts`](./src/modules/order/application/create-order.use-case.ts): Criar o `MessageWrapper` e salvar o evento `'ORDER_WORKFLOW_REQUESTED'` na Outbox dentro da `unitOfWork`.
2. [`src/modules/order/application/execute-saga-step.use-case.ts`](./src/modules/order/application/execute-saga-step.use-case.ts): Implementar a coordenação agnóstica `Inbox` ➔ `UseCase de Domínio` ➔ `Outbox ('WORKFLOW_STEP_COMPLETED' | 'WORKFLOW_STEP_FAILED')`.
3. [`src/modules/order/application/relay-outbox.use-case.ts`](./src/modules/order/application/relay-outbox.use-case.ts): Ler mensagens `PENDING` da Outbox e chamar `this._orderWorkflow` (`startOrderWorkflow`, `completeWorkflowStep`, `failWorkflowStep`).
4. [`src/infra/aws/translators/sfn-acl.translator.ts`](./src/infra/aws/translators/sfn-acl.translator.ts): Implementar a camada de tradução (ACL) entre os tipos da AWS (`taskToken`, `executionArn`, `payload.error`) e os tipos agnósticos da aplicação (`correlationToken`, `workflowInstanceId`, `payload.failure`).
5. [`src/infra/aws/sfn-order-workflow.adapter.ts`](./src/infra/aws/sfn-order-workflow.adapter.ts) e [`src/infra/aws/sqs-saga-step.worker.ts`](./src/infra/aws/sqs-saga-step.worker.ts): Conectar os comandos do `@aws-sdk/client-sfn` e `@aws-sdk/client-sqs`.
6. [`infra/statemachine/order-saga.asl.json`](./infra/statemachine/order-saga.asl.json): Completar os estados da State Machine (`ReserveStock` ➔ `ProcessPayment` ➔ `CreateShipment` ➔ `ConfirmOrder` e os rollbacks `RefundPayment` ➔ `ReleaseStock` ➔ `CancelOrder`) e rodar `cd infra && terraform apply -auto-approve`.

> **Dica:** A qualquer momento você pode comparar sua implementação com o gabarito na `master`:
> ```bash
> git diff master -- src/modules/order/application/execute-saga-step.use-case.ts
> ```

---

## 3. Como Testar Cada Etapa e os Rollbacks (`curl`)

Antes de começar (ou entre um teste e outro), você pode conferir o estoque inicial ou resetar o banco:

```bash
# Ver saldo atual do estoque (SKU-100 começa com availableQuantity = 10, reservedQuantity = 0)
curl -s http://localhost:3000/stock | jq

# Resetar todas as tabelas do PostgreSQL para o estado inicial a qualquer momento
curl -s -X POST http://localhost:3000/db/reset | jq
```

---

### Cenário 1: Caminho Feliz (`ReserveStock` ➔ `ProcessPayment` ➔ `CreateShipment` ➔ `ConfirmOrder`)

**1. Bater no endpoint de criação do pedido:**
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
- `workflowExecution.status`: `"COMPLETED"`
- `transactionalMessaging.inboxProcessedSteps`: 4 steps processados (`RESERVE_STOCK`, `PROCESS_PAYMENT`, `CREATE_SHIPMENT`, `CONFIRM_ORDER`)
- `distributedTracing.traceIds`: `["1000000000000000001"]`

---

### Cenário 2: Falha no Passo 1 — Estoque Insuficiente (`ReserveStock [ERR]` ➔ `CancelOrder`)

Solicitamos `quantity: 50` (maior que o saldo disponível de 10):

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
- `workflowExecution.status`: `"ROLLED_BACK"`
- `transactionalMessaging.inboxProcessedSteps`: apenas `RESERVE_STOCK` (`ERR`) e `CANCEL_ORDER` (`OK`).

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

## 4. Como Inspecionar o MiniStack (AWS CLI) e o Banco PostgreSQL (`psql`)

### 4.1 Inspecionar a State Machine e as Execuções no MiniStack via AWS CLI
```bash
# 1. Listar as State Machines criadas pelo Terraform no MiniStack
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url=http://localhost:4566 --region us-east-1 \
  stepfunctions list-state-machines | jq

# 2. Listar todas as execuções da Order Saga no MiniStack
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url=http://localhost:4566 --region us-east-1 \
  stepfunctions list-executions \
  --state-machine-arn arn:aws:states:us-east-1:000000000000:stateMachine:order-saga-state-machine | jq

# 3. Ver o histórico passo a passo de uma execução específica no Step Functions
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url=http://localhost:4566 --region us-east-1 \
  stepfunctions get-execution-history \
  --execution-arn arn:aws:states:us-east-1:000000000000:execution:order-saga-state-machine:<ORDER_ID> | jq
```

### 4.2 Inspecionar as Tabelas Reais no PostgreSQL via `psql`
```bash
# 1. Ver eventos da Transactional Outbox (ORDER_WORKFLOW_REQUESTED, WORKFLOW_STEP_COMPLETED, WORKFLOW_STEP_FAILED)
docker exec -it step-function-postgres psql -U saga_user -d order_saga_db -c \
  "SELECT id, order_id, event_type, status, workflow_instance_id, created_at, published_at FROM outbox_messages ORDER BY created_at;"

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

### 4.3 Inspecionar via Endpoint HTTP (`GET /db`)
```bash
curl -s http://localhost:3000/db | jq
```
