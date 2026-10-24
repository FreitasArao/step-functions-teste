import type { Payment } from './payment.entity.js';

export const PAYMENT_REPOSITORY_PORT = Symbol('PAYMENT_REPOSITORY_PORT');

export interface PaymentRepositoryPort {
  save(payment: Payment): Promise<void>;
  findByOrderId(orderId: string): Promise<Payment | null>;
  findAll(): Promise<Payment[]>;
}
