import axios from 'axios';
import type { CreatePaymentRequest, PaymentResponse } from '../types';

// JWT token loaded from environment variable (set VITE_DEV_JWT in .env.local for dev)
// In production this would come from an auth flow (OAuth2/OIDC)
const DEV_JWT = import.meta.env.VITE_DEV_JWT ?? '';

const api = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json',
    ...(DEV_JWT ? { Authorization: `Bearer ${DEV_JWT}` } : {}),
  },
});

export async function createPayment(
  request: CreatePaymentRequest,
  idempotencyKey: string
): Promise<PaymentResponse> {
  const { data } = await api.post<PaymentResponse>('/payments', request, {
    headers: { 'Idempotency-Key': idempotencyKey },
  });
  return data;
}

export async function getPayment(paymentId: string): Promise<PaymentResponse> {
  const { data } = await api.get<PaymentResponse>(`/payments/${paymentId}`);
  return data;
}

// Ping the API gateway health endpoint
export async function pingGateway(): Promise<boolean> {
  try {
    const resp = await axios.get('/management/actuator/health', { timeout: 3000 });
    return resp.status === 200;
  } catch (err: unknown) {
    if (axios.isAxiosError(err) && err.response) {
      // Got a response (even 401/404) — server is up
      return true;
    }
    return false;
  }
}
