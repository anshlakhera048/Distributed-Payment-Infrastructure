export interface PaymentEvent {
  version: string;
  eventId: string;
  traceId: string;
  correlationId: string;
  eventType: string;
  paymentId: string;
  userId: string;
  merchantId: string;
  amount: number;
  currency: string;
  status: string;
  idempotencyKey: string;
  description: string;
  createdAt: string;
  eventTimestamp: string;
}

export interface PaymentResponse {
  paymentId: string;
  userId: string;
  merchantId: string;
  amount: number;
  currency: string;
  status: 'PENDING' | 'SUCCESS' | 'FAILED' | 'FRAUD_REJECTED';
  idempotencyKey: string;
  description: string;
  createdAt: string;
}

export interface CreatePaymentRequest {
  userId: string;
  merchantId: string;
  amount: number;
  currency: string;
  description?: string;
}

export interface SystemStatus {
  apiOnline: boolean;
  wsConnected: boolean;
  lastEventAt: string | null;
}

export interface AlertItem {
  id: string;
  type: 'fraud' | 'failure' | 'webhook';
  message: string;
  paymentId: string;
  timestamp: string;
}

export interface Metrics {
  totalPayments: number;
  successCount: number;
  failedCount: number;
  fraudCount: number;
  pendingCount: number;
}
