import { Injectable } from '@nestjs/common';
import type { Shipment } from '../domain/shipment.entity.js';
import type { ShipmentRepositoryPort } from '../domain/shipment.repository.port.js';

@Injectable()
export class InMemoryShipmentRepository implements ShipmentRepositoryPort {
  private readonly shipmentsByOrder = new Map<string, Shipment>();

  async save(shipment: Shipment): Promise<void> {
    this.shipmentsByOrder.set(shipment.orderId, shipment);
  }

  async findByOrderId(orderId: string): Promise<Shipment | null> {
    return this.shipmentsByOrder.get(orderId) ?? null;
  }

  async findAll(): Promise<Shipment[]> {
    return Array.from(this.shipmentsByOrder.values());
  }

  clear(): void {
    this.shipmentsByOrder.clear();
  }
}
