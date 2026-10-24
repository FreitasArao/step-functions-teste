import { Inject, Injectable } from '@nestjs/common';
import {
  TELEMETRY_PORT,
  type TelemetryPort,
} from '../../../infra/telemetry/tracer.js';
import type { DomainError } from '../../../shared/core/domain-error.js';
import { Result } from '../../../shared/core/result.js';
import {
  UNIT_OF_WORK_PORT,
  type UnitOfWorkPort,
} from '../../../shared/messaging/unit-of-work.port.js';
import { ProcessPaymentUseCase } from '../../payment/application/process-payment.use-case.js';
import { RefundPaymentUseCase } from '../../payment/application/refund-payment.use-case.js';
import { CreateShipmentUseCase } from '../../shipping/application/create-shipment.use-case.js';
import { ReleaseStockUseCase } from '../../stock/application/release-stock.use-case.js';
import { ReserveStockUseCase } from '../../stock/application/reserve-stock.use-case.js';
import type { WorkflowStepCommand } from '../domain/order-workflow.port.js';
import { CancelOrderUseCase } from './cancel-order.use-case.js';
import { ConfirmOrderUseCase } from './confirm-order.use-case.js';

/**
 * TODO (Prática - Caso de Uso de Orquestração de Step da Saga - 100% Agnóstico de AWS):
 * Recebe um WorkflowStepCommand já traduzido pela ACL e coordena:
 * 1. Span de trace distribuído (this._telemetry.runInSpan)
 * 2. UnitOfWork (Inbox idempotência -> UseCase de domínio Result<T,E> -> Outbox WORKFLOW_STEP_COMPLETED / WORKFLOW_STEP_FAILED)
 */
@Injectable()
export class ExecuteSagaStepUseCase {
  constructor(
    @Inject(UNIT_OF_WORK_PORT)
    private readonly _unitOfWork: UnitOfWorkPort,
    @Inject(TELEMETRY_PORT)
    private readonly _telemetry: TelemetryPort,
    private readonly _reserveStockUseCase: ReserveStockUseCase,
    private readonly _releaseStockUseCase: ReleaseStockUseCase,
    private readonly _processPaymentUseCase: ProcessPaymentUseCase,
    private readonly _refundPaymentUseCase: RefundPaymentUseCase,
    private readonly _createShipmentUseCase: CreateShipmentUseCase,
    private readonly _confirmOrderUseCase: ConfirmOrderUseCase,
    private readonly _cancelOrderUseCase: CancelOrderUseCase,
  ) {}

  async execute(
    _command: WorkflowStepCommand,
  ): Promise<
    Result<{ deduplicated: boolean; output?: unknown }, DomainError>
  > {
    // TODO: Implementar fluxo Transactional Inbox -> Domain UseCase -> Transactional Outbox
    return Result.ok({ deduplicated: false });
  }
}
