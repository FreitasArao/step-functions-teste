import { Injectable } from '@nestjs/common';
import type { Order } from '../domain/order.entity.js';
import type { OrderRepositoryPort } from '../domain/order.repository.port.js';

@Injectable()
export class InMemoryOrderRepository implements OrderRepositoryPort {
  private readonly orders = new Map<string, Order>();

  async save(order: Order): Promise<void> {
    this.orders.set(order.id, order);
  }

  async findById(orderId: string): Promise<Order | null> {
    return this.orders.get(orderId) ?? null;
  }

  async findAll(): Promise<Order[]> {
    return Array.from(this.orders.values());
  }

  clear(): void {
    this.orders.clear();
  }
}
