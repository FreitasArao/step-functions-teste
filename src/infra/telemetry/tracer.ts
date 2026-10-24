import { Injectable } from '@nestjs/common';
import { randomBytes } from 'node:crypto';
import tracer from 'dd-trace';
import type { TraceContextCarrier } from '../../shared/messaging/message-wrapper.js';

let initialized = false;

export function initDatadogTracer(): void {
  if (initialized) {
    return;
  }
  initialized = true;
  try {
    tracer.init({
      service: process.env.DD_SERVICE ?? 'order-saga-step-functions',
      env: process.env.DD_ENV ?? 'local-ministack',
      version: '1.0.0',
      logInjection: true,
      startupLogs: false,
    });
  } catch {
    // Graceful fallback if dd-trace cannot bind local agent socket
  }
}

initDatadogTracer();

export interface RecordedSpan {
  readonly traceId: string;
  readonly spanId: string;
  readonly parentSpanId?: string;
  readonly operationName: string;
  readonly resourceName: string;
  readonly correlationId: string;
  readonly startedAt: string;
  readonly finishedAt: string;
  readonly status: 'OK' | 'ERR';
  readonly tags: Record<string, string | number | boolean>;
  readonly injectedCarrier: TraceContextCarrier;
}

export const TELEMETRY_PORT = Symbol('TELEMETRY_PORT');

export interface TelemetryPort {
  runInSpan<T>(
    params: {
      operationName: string;
      resourceName: string;
      correlationId: string;
      parentCarrier?: TraceContextCarrier;
      tags?: Record<string, string | number | boolean>;
    },
    fn: (carrier: TraceContextCarrier) => Promise<T>,
  ): Promise<T>;
  getSpansByCorrelationId(correlationId: string): RecordedSpan[];
  getSpansByTraceId(traceId: string): RecordedSpan[];
  getAllSpans(): RecordedSpan[];
  clear(): void;
}

function generate64BitDecimalId(): string {
  // Valid 63-bit positive integer in decimal representation (Datadog trace/span id format)
  const buf = randomBytes(8);
  buf[0] = buf[0] & 0x7f;
  const value = buf.readBigUInt64BE(0);
  return (value === 0n ? 1n : value).toString(10);
}

function toHex16(decimalStr: string): string {
  try {
    return BigInt(decimalStr).toString(16).padStart(16, '0');
  } catch {
    return '0000000000000001';
  }
}

@Injectable()
export class DatadogTelemetryService implements TelemetryPort {
  private readonly recordedSpans: RecordedSpan[] = [];

  async runInSpan<T>(
    params: {
      operationName: string;
      resourceName: string;
      correlationId: string;
      parentCarrier?: TraceContextCarrier;
      tags?: Record<string, string | number | boolean>;
    },
    fn: (carrier: TraceContextCarrier) => Promise<T>,
  ): Promise<T> {
    const startedAt = new Date().toISOString();
    const parentContext = params.parentCarrier
      ? tracer.extract('text_map', params.parentCarrier)
      : null;

    const span = tracer.startSpan(params.operationName, {
      ...(parentContext ? { childOf: parentContext } : {}),
      tags: {
        'resource.name': params.resourceName,
        'correlation.id': params.correlationId,
        ...params.tags,
      },
    });

    const injectedCarrier: TraceContextCarrier = {};
    tracer.inject(span, 'text_map', injectedCarrier);

    // Ensure Datadog & W3C headers are always populated and linked even without local agent daemon
    const inheritedTraceId =
      params.parentCarrier?.['x-datadog-trace-id'] ??
      injectedCarrier['x-datadog-trace-id'] ??
      generate64BitDecimalId();

    const parentSpanId = params.parentCarrier?.['x-datadog-parent-id'];
    const currentSpanId =
      injectedCarrier['x-datadog-parent-id'] &&
      injectedCarrier['x-datadog-parent-id'] !== parentSpanId
        ? injectedCarrier['x-datadog-parent-id']
        : generate64BitDecimalId();

    injectedCarrier['x-datadog-trace-id'] = inheritedTraceId;
    injectedCarrier['x-datadog-parent-id'] = currentSpanId;
    injectedCarrier['x-datadog-sampling-priority'] =
      injectedCarrier['x-datadog-sampling-priority'] ?? '1';
    injectedCarrier.traceparent = `00-${toHex16(inheritedTraceId).padStart(32, '0')}-${toHex16(currentSpanId)}-01`;

    let status: 'OK' | 'ERR' = 'OK';
    const finalTags: Record<string, string | number | boolean> = {
      ...params.tags,
    };

    try {
      const result = await tracer.scope().activate(span, () =>
        fn({ ...injectedCarrier }),
      );
      if (
        result &&
        typeof result === 'object' &&
        'isErr' in result &&
        typeof (result as { isErr: () => boolean }).isErr === 'function' &&
        (result as { isErr: () => boolean }).isErr()
      ) {
        status = 'ERR';
        const errObj = (result as { error?: { code?: string; message?: string } })
          .error;
        if (errObj?.code) {
          finalTags['error.code'] = errObj.code;
          span.setTag('error.type', errObj.code);
        }
        if (errObj?.message) {
          finalTags['error.message'] = errObj.message;
          span.setTag('error.msg', errObj.message);
        }
      }
      return result;
    } catch (err) {
      status = 'ERR';
      span.setTag('error', err);
      throw err;
    } finally {
      span.finish();
      this.recordedSpans.push({
        traceId: inheritedTraceId,
        spanId: currentSpanId,
        ...(parentSpanId ? { parentSpanId } : {}),
        operationName: params.operationName,
        resourceName: params.resourceName,
        correlationId: params.correlationId,
        startedAt,
        finishedAt: new Date().toISOString(),
        status,
        tags: finalTags,
        injectedCarrier: { ...injectedCarrier },
      });
    }
  }

  getSpansByCorrelationId(correlationId: string): RecordedSpan[] {
    return this.recordedSpans.filter((s) => s.correlationId === correlationId);
  }

  getSpansByTraceId(traceId: string): RecordedSpan[] {
    return this.recordedSpans.filter((s) => s.traceId === traceId);
  }

  getAllSpans(): RecordedSpan[] {
    return [...this.recordedSpans];
  }

  clear(): void {
    this.recordedSpans.length = 0;
  }
}
