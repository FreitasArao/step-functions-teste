import type { Shipment } from './shipment.entity.js';

export const SHIPMENT_REPOSITORY_PORT = Symbol('SHIPMENT_REPOSITORY_PORT');

export interface ShipmentRepositoryPort {
  save(shipment: Shipment): Promise<void>;
  findByOrderId(orderId: string): Promise<Shipment | null>;
  findAll(): Promise<Shipment[]>;
}
