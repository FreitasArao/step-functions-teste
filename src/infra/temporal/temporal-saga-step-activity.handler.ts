import { Inject, Injectable } from '@nestjs/common';
import {
  EXECUTE_SAGA_STEP_INBOUND_PORT,
  type ExecuteSagaStepInboundPort,
  type ExecuteSagaStepOutput,
} from '../../modules/order/application/ports/saga-step.inbound-port.js';
import {
  type TemporalActivityEnvelope,
  TemporalAclTranslator,
} from './translators/temporal-acl.translator.js';

/**
 * Inbox side of the Temporal integration.
 *
 * Invoked directly from inside the Temporal Activity (the activity task is persisted by the
 * Temporal Server, so Temporal IS the durable queue — there is no volatile in-memory hop).
 *
 * Translates the Temporal envelope (ACL) and runs ExecuteSagaStepInboundPort, which commits
 * Inbox + Domain + Outbox(callback) atomically.
 *
 * Throws on infrastructure failure (DB down, etc.) so Temporal RETRIES the activity; the Inbox
 * makes those retries idempotent.
 */
@Injectable()
export class TemporalSagaStepActivityHandler {
  constructor(
    private readonly translator: TemporalAclTranslator,
    @Inject(EXECUTE_SAGA_STEP_INBOUND_PORT)
    private readonly executeSagaStepPort: ExecuteSagaStepInboundPort,
  ) {}

  async handle(
    envelope: TemporalActivityEnvelope | string,
  ): Promise<ExecuteSagaStepOutput> {
    const command = this.translator.toWorkflowStepCommand(envelope);
    const result = await this.executeSagaStepPort.execute(command);
    if (result.isErr()) {
      throw new Error(
        `[${result.error.code}] Saga step '${command.stepName}' could not be processed: ${result.error.message}`,
      );
    }
    return result.value;
  }
}
