# Order Saga — Temporal.io (`AsyncCompletionClient`) vs AWS Step Functions + Outbox/Inbox + DDD + Result Pattern + `dd-trace`

Laboratório prático (**build to learn**) na branch `feat/temporal-io` demonstrando a substituição completa do **AWS Step Functions + SQS + Terraform** pelo **Temporal.io** para responder na prática:

> **"Quão bom e desacoplável está o `MessageWrapper<T>` e o conjunto de Ports (`OrderWorkflowPort`, `ExecuteSagaStepInboundPort`, `MessagePublisherPort`, `Inbox`, `Outbox`, `UnitOfWork`)?"**

---

## 1. Veredito Arquitetural: Quão Desacoplado Está o `MessageWrapper` e as Ports?

Para colocar a arquitetura à prova sem "roubar" no teste, migramos de **AWS Step Functions (`waitForTaskToken` + SQS + ASL JSON)** para **Temporal.io (TypeScript Workflows + Async Activity Completion)** mantendo a regra: **proibido alterar qualquer regra de negócio, caso de uso, contrato de mensageria ou porta**.

### Resultado Real do `git diff main` por Camada

| Camada / Diretório | Arquivos | Linhas Alteradas | Veredito |
| :--- | :--- | :---: | :--- |
| **`src/shared/messaging/**`** | [`message-wrapper.ts`](./src/shared/messaging/message-wrapper.ts), [`outbox-relay.service.ts`](./src/shared/messaging/outbox-relay.service.ts), [`inbox.port.ts`](./src/shared/messaging/inbox.port.ts), [`outbox.port.ts`](./src/shared/messaging/outbox.port.ts), [`unit-of-work.port.ts`](./src/shared/messaging/unit-of-work.port.ts) | **0 linhas** | **100% Desacoplado** |
| **`src/shared/core/**`** | [`result.ts`](./src/shared/core/result.ts), [`domain-error.ts`](./src/shared/core/domain-error.ts) | **0 linhas** | **100% Desacoplado** |
| **`src/modules/**/domain/**`** | Entidades (`Order`, `StockItem`, `Payment`, `Shipment`), Erros e Repository Ports | **0 linhas** | **100% Desacoplado** |
| **`src/modules/**/application/**`** | [`create-order.use-case.ts`](./src/modules/order/application/create-order.use-case.ts), [`execute-saga-step.use-case.ts`](./src/modules/order/application/execute-saga-step.use-case.ts), [`get-order-diagnostics.use-case.ts`](./src/modules/order/application/get-order-diagnostics.use-case.ts), [`order-workflow.outbound-port.ts`](./src/modules/order/application/ports/order-workflow.outbound-port.ts), [`saga-step.inbound-port.ts`](./src/modules/order/application/ports/saga-step.inbound-port.ts) | **0 linhas** | **100% Desacoplado** |
| **`src/modules/order/infra/http/`** | [`order.controller.ts`](./src/modules/order/infra/http/order.controller.ts) | **1 linha** (`import SagaPipelineRunnerService`) | Único ponto de melhoria (ver abaixo) |
| **`src/infra/temporal/**`** | Novos Adapters, Worker, Workflow TS e [`TemporalAclTranslator`](./src/infra/temporal/translators/temporal-acl.translator.ts) | Substituiu `src/infra/aws/**` | Plug & Play via Injeção de Dependência |

```bash
# Você mesmo pode auditar com um único comando:
git diff main -- src/shared/ src/modules/order/application/ src/modules/order/domain/ src/modules/stock/ src/modules/payment/ src/modules/shipping/
# (Saída vazia = 0 linhas alteradas!)
```

### Por que o `MessageWrapper<T>` funcionou tão bem na troca para o Temporal.io?

1. **Zero Vazamento de Protocolo (`taskToken` / `executionArn` / `runId` fora do Wrapper):**
   - O [`MessageWrapper<T>`](./src/shared/messaging/message-wrapper.ts) carrega apenas metadados agnósticos (`messageId`, `correlationId`, `causationId`, `timestamp`, `source`, `traceContext`) e o `payload` de domínio.
   - Enquanto na AWS o `taskToken` é uma string opaca gerada pelo Step Functions no SQS, no Temporal o `taskToken` é um `Uint8Array` binário dentro de `activityInfo().taskToken`.
   - Graças ao [`WorkflowStepCommand`](./src/modules/order/application/ports/saga-step.inbound-port.ts) e à ACL ([`TemporalAclTranslator`](./src/infra/temporal/translators/temporal-acl.translator.ts)), o `Uint8Array` do Temporal é codificado em Base64 como `correlationToken` na entrada e decodificado de volta para `Uint8Array` quando o [`ExecuteSagaStepUseCase`](./src/modules/order/application/execute-saga-step.use-case.ts) chama `orderWorkflow.completeWorkflowStep` ou `failWorkflowStep`.
2. **Propagação de Trace Distribuído (`dd-trace`) Intacta:**
   - Como o `traceContext` (`x-datadog-trace-id`, `x-datadog-parent-id`, `traceparent`) viaja dentro de `wrapper.metadata.traceContext`, o trace iniciado no `POST /orders` atravessa a tabela `outbox_messages`, entra no Workflow do Temporal, desce para cada Activity (`RESERVE_STOCK`, `PROCESS_PAYMENT`, etc.) e é gravado na `inbox_messages` mantendo exatamente o mesmo `traceId` de ponta a ponta!
3. **Normalização de Falhas na Compensação (`payload.failure`):**
   - No Step Functions, o bloco `Catch` injeta `payload.error = { Error, Cause }`.
   - No Temporal, uma Activity falha lançando `ApplicationFailure.nonRetryable(message, code, details)` capturada como `ActivityFailure` no `try/catch` do Workflow.
   - O [`TemporalAclTranslator`](./src/infra/temporal/translators/temporal-acl.translator.ts) converte o erro do Temporal para a mesma estrutura agnóstica `payload.failure = { code, message }` que o `CancelOrderUseCase` espera.
4. **Único Ponto de Acoplamento Encontrado (Lição Aprendida):**
   - No [`OrderController`](./src/modules/order/infra/http/order.controller.ts), a classe `SagaPipelineRunnerService` estava sendo importada pelo caminho físico de `infra/aws`. Se ela fosse injetada por um token de porta (ex.: `SAGA_PIPELINE_RUNNER_PORT`), nem essa única linha de `import` no controller HTTP precisaria mudar.

---

## 2. Comparativo Técnico: AWS Step Functions (`main`) vs Temporal.io (`feat/temporal-io`)

| Dimensão | AWS Step Functions + SQS (`main`) | Temporal.io (`feat/temporal-io`) |
| :--- | :--- | :--- |
| **Definição da Saga** | JSON declarativo (`order-saga.asl.json` com ~180 linhas de estados, `ResultPath` e `Catch` encadeados) | Código TypeScript determinístico ([`order-saga.workflow.ts`](./src/infra/temporal/workflows/order-saga.workflow.ts)) com pilha LIFO `compensations.unshift(...)` e `try/catch` nativo |
| **Pausa / Callback Assíncrono** | `arn:aws:states:::sqs:sendMessage.waitForTaskToken` + `SendTaskSuccessCommand` / `SendTaskFailureCommand` | `throw new CompleteAsyncError()` na Activity ([`order-saga.activities.ts`](./src/infra/temporal/activities/order-saga.activities.ts)) + `AsyncCompletionClient.complete(taskToken)` / `fail(taskToken)` |
| **Camada Anticorrupção (ACL)** | `AwsStepFunctionsTranslator` (`sfn-acl.translator.ts`) | [`TemporalAclTranslator`](./src/infra/temporal/translators/temporal-acl.translator.ts) |
| **Adapter de `OrderWorkflowPort`** | `StepFunctionsOrderWorkflowAdapter` (`@aws-sdk/client-sfn`) | [`TemporalOrderWorkflowAdapter`](./src/infra/temporal/temporal-order-workflow.adapter.ts) (`@temporalio/client`) |
| **Provisionamento Local** | LocalStack + Terraform (`terraform init && terraform apply`) | Apenas `docker compose up -d` (Zero Terraform — Workflows são registrados pelo Worker em código) |
| **Observabilidade Visual** | AWS CLI (`stepfunctions get-execution-history`) | **Temporal Web UI** (`http://localhost:8088`) + Temporal CLI |

### Como ficou o Rollback em Cascata no Temporal (`order-saga.workflow.ts`)?

Em vez de 7 estados JSON interligados manualmente por `Catch`, no Temporal usamos o padrão clássico de **Pilha de Compensação LIFO** em TypeScript:

```ts
const compensations: Array<() => Promise<void>> = [];

try {
  // Passo 1: Reserva Estoque
  statePayload.stock = await dispatchSagaStepActivity({ stepName: 'RESERVE_STOCK', ... });
  // Registra Compensação Nível 1
  compensations.unshift(async () => {
    await dispatchSagaStepActivity({ stepName: 'RELEASE_STOCK', ... });
  });

  // Passo 2: Processa Pagamento
  statePayload.payment = await dispatchSagaStepActivity({ stepName: 'PROCESS_PAYMENT', ... });
  // Registra Compensação Nível 2 (no topo da pilha, roda ANTES do ReleaseStock!)
  compensations.unshift(async () => {
    await dispatchSagaStepActivity({ stepName: 'REFUND_PAYMENT', ... });
  });

  // Passo 3 e 4: Logística e Confirmação
  statePayload.shipment = await dispatchSagaStepActivity({ stepName: 'CREATE_SHIPMENT', ... });
  statePayload.confirmation = await dispatchSagaStepActivity({ stepName: 'CONFIRM_ORDER', ... });
} catch (err) {
  // Executa compensações em cascata na ordem inversa (Nível 2 -> Nível 1)
  for (const runCompensation of compensations) {
    await runCompensation();
  }
  // Cancela o pedido com o motivo da falha
  await dispatchSagaStepActivity({ stepName: 'CANCEL_ORDER', ... });
  throw ApplicationFailure.nonRetryable('Order saga failed...', 'ORDER_SAGA_ROLLED_BACK');
}
```

---

## 3. Fluxo Completo ao Bater em `POST /orders` com Temporal.io (Transactional Outbox + Inbox Puros)

```text
1. [HTTP POST /orders]
     │
     ▼
2. [CreateOrderUseCase + PostgresUnitOfWork] (Transação ACID #1)
     ├── Salva Order (status = 'PENDING') na tabela `orders`
     └── Salva evento de domínio 'ORDER_CREATED' (status = 'PENDING') com MessageWrapper na `outbox_messages`
     │
     ▼
3. [OutboxRelayWorker -> OutboxRelayService] (100% Genérico + `FOR UPDATE SKIP LOCKED`)
     ├── Lê mensagens 'PENDING' da `outbox_messages` dentro de uma transação
     ├── Publica via `MessagePublisherPort` (`TemporalEventPublisherAdapter` -> `OrderWorkflowPort.startOrderWorkflow`)
     │   (idempotente no Temporal via `workflowId: order-saga-<orderId>` + `REJECT_DUPLICATE`)
     └── Marca como 'PUBLISHED' SOMENTE após o ACK do Temporal Server
     │
     ▼
4. [Temporal Worker -> Activity (`dispatchSagaStepActivity`)]
     ├── O Workflow entra no passo ('RESERVE_STOCK', 'PROCESS_PAYMENT', etc.)
     ├── A Activity captura o `activityInfo().taskToken` binário do Temporal (em Base64)
     ├── Invoca diretamente `TemporalSagaStepActivityHandler.handle(envelope)` (sem fila em memória!)
     └── Após a confirmação do callback, lança `CompleteAsyncError`
     │
     ▼
5. [TemporalAclTranslator -> ExecuteSagaStepUseCase] (Transação ACID #2 — Apenas Inbox + Domínio)
     ├── Traduz o envelope do Temporal para `WorkflowStepCommand` agnóstico (`correlationToken`)
     ├── Abre transação ACID no Postgres (`PostgresUnitOfWork`):
     │     1. Verifica idempotência na tabela `inbox_messages` (`orderId:stepName`)
     │     2. Executa o Use Case de Domínio puro (`Result<T, DomainError>`) — apenas na 1ª entrega
     │     3. Salva na `inbox_messages` (`outcome = 'OK'` ou `'ERR'` + `responsePayload`)
     └── Commita a transação (NÃO suja a Outbox com callbacks de step!)
     │
     ▼
6. [Callback via `OrderWorkflowPort` (`AsyncCompletionClient` do Temporal)]
     ├── Se 'OK'  -> `AsyncCompletionClient.complete(taskToken, output)` -> avança para a próxima Activity!
     ├── Se 'ERR' -> `AsyncCompletionClient.fail(taskToken, ApplicationFailure)` -> cai no `catch` do Workflow e roda a pilha `compensations`!
     └── Se o callback de rede falhar, a Activity falha e o Temporal reentrega a mensagem:
         na reentrega, a `inbox_messages` evita reexecutar o domínio (`deduplicated = true`) e apenas reenvia o callback!
```

---

## 4. Tutorial Passo a Passo: Como Subir e Testar Tudo

### Passo 4.1 — Rodar os Testes Automatizados (Unitários + Saga Completa)
Mesmo sem Docker, todos os 5 cenários da Saga (incluindo Rollback de 1 e 2 níveis e Idempotência de Inbox com o `TemporalAclTranslator`) podem ser validados instantaneamente:
```bash
bun run test
```

### Passo 4.2 — Subir PostgreSQL 16 + Temporal Server + Temporal Web UI via Docker Compose
Na raiz do projeto, execute:
```bash
docker compose up -d
```
Verifique se os 3 containers estão rodando:
- `step-function-postgres` (porta `5432`)
- `order-saga-temporal` (porta `7233` — gRPC do Temporal Server)
- `order-saga-temporal-ui` (porta `8088` — interface visual do Temporal no navegador: `http://localhost:8088`)

```bash
docker compose ps
```
> **Nota:** Diferente da branch `main` (Step Functions), **não é necessário rodar Terraform**! No Temporal.io a definição do Workflow é código TypeScript ([`order-saga.workflow.ts`](./src/infra/temporal/workflows/order-saga.workflow.ts)) registrado automaticamente pelo Worker da aplicação.

### Passo 4.3 — Iniciar a Aplicação NestJS
```bash
bun run start:dev
```
A aplicação subirá em `http://localhost:3000`, conectará ao PostgreSQL (`order_saga_db`), conectará ao Temporal Server (`localhost:7233`) e iniciará o Temporal Worker escutando a fila `order-saga-task-queue`.

---

## 5. Como Testar Cada Etapa e os Rollbacks (`curl`)

Antes de começar (ou entre um teste e outro), você pode conferir o estoque inicial ou resetar o banco:

```bash
# Ver saldo atual do estoque (SKU-100 começa com availableQuantity = 10, reservedQuantity = 0)
curl -s http://localhost:3000/stock | jq

# Resetar todas as tabelas do PostgreSQL para o estado inicial a qualquer momento
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

**2. Consultar o diagnóstico completo do pedido (substitua `<ORDER_ID>` pelo `order.id` retornado):**
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
- `workflowExecution.workflowInstanceId`: `"order-saga-<ORDER_ID>:<RUN_ID>"`
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

**Validar o resultado (`GET /orders/<ORDER_ID>`):**
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

## 6. Como Inspecionar o Temporal (Web UI + CLI) e o PostgreSQL (`psql`)

### 6.1 Inspecionar na Temporal Web UI
Abra no navegador:
- **`http://localhost:8088`**
- Você verá todas as execuções `order-saga-<ORDER_ID>`, o `Memo` com o `correlationId` e `traceContext`, e a linha do tempo detalhada de cada `dispatchSagaStepActivity` e suas compensações!

### 6.2 Inspecionar Workflows via Temporal CLI (dentro do container)
```bash
# 1. Listar todas as execuções de Workflow no Temporal
docker exec -it order-saga-temporal temporal workflow list --address temporal:7233

# 2. Ver o histórico de eventos passo a passo de um pedido específico
docker exec -it order-saga-temporal temporal workflow show \
  --workflow-id order-saga-<ORDER_ID> \
  --address temporal:7233
```

### 6.3 Inspecionar as Tabelas Reais no PostgreSQL via `psql`
```bash
# 1. Ver eventos da Transactional Outbox (ORDER_CREATED)
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

### 6.4 Inspecionar via Endpoint HTTP (`GET /db`)
```bash
curl -s http://localhost:3000/db | jq
```
