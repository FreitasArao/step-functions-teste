import { Inject, Injectable } from '@nestjs/common';
import {
  TELEMETRY_PORT,
  type TelemetryPort,
} from '../../../infra/telemetry/tracer.js';
import {
  OUTBOX_REPOSITORY_PORT,
  type OutboxRepositoryPort,
} from '../../../shared/messaging/outbox.port.js';
import {
  ORDER_WORKFLOW_PORT,
  type OrderWorkflowPort,
} from '../domain/order-workflow.port.js';

/**
 * TODO (Prática - Caso de Uso de Relay da Outbox - 100% Agnóstico de AWS):
 * Lê eventos PENDING da OutboxRepositoryPort e despacha através da OrderWorkflowPort:
 * - 'ORDER_WORKFLOW_REQUESTED' -> orderWorkflow.startOrderWorkflow(...)
 * - 'WORKFLOW_STEP_COMPLETED'  -> orderWorkflow.completeWorkflowStep(...)
 * - 'WORKFLOW_STEP_FAILED'     -> orderWorkflow.failWorkflowStep(...)
 */
@Injectable()
export class RelayOutboxUseCase {
  constructor(
    @Inject(OUTBOX_REPOSITORY_PORT)
    private readonly _outboxRepository: OutboxRepositoryPort,
    @Inject(ORDER_WORKFLOW_PORT)
    private readonly _orderWorkflow: OrderWorkflowPort,
    @Inject(TELEMETRY_PORT)
    private readonly _telemetry: TelemetryPort,
  ) {}

  async executeBatch(): Promise<number> {
    // TODO: Implementar processamento da Outbox com span filho do Datadog e markPublished
    return 0;
  }
}
