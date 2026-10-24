import { Injectable } from '@nestjs/common';
import type { Payment } from '../domain/payment.entity.js';
import type { PaymentRepositoryPort } from '../domain/payment.repository.port.js';

@Injectable()
export class InMemoryPaymentRepository implements PaymentRepositoryPort {
  private readonly paymentsByOrder = new Map<string, Payment>();

  async save(payment: Payment): Promise<void> {
    this.paymentsByOrder.set(payment.orderId, payment);
  }

  async findByOrderId(orderId: string): Promise<Payment | null> {
    return this.paymentsByOrder.get(orderId) ?? null;
  }

  async findAll(): Promise<Payment[]> {
    return Array.from(this.paymentsByOrder.values());
  }

  clear(): void {
    this.paymentsByOrder.clear();
  }
}
