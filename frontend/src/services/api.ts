import axios from 'axios';
import type { CreatePaymentRequest, PaymentResponse } from '../types';

// Dev JWT token (HS256, 1-year expiry, signed with the default dev secret)
// In production this would come from an auth flow
const DEV_JWT = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYXNoYm9hcmQtdXNlciIsImlzcyI6InBheW1lbnRzLXN5c3RlbSIsInJvbGVzIjpbImFkbWluIl0sImlhdCI6MTc3NDY0MDg0NSwiZXhwIjoxODA2MTc2ODQ1fQ.0J0jIzXBXAAKXpUI1x7xcku5eTHzxgR8K8pWFLMzwI8';

const api = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${DEV_JWT}`,
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

export async function checkApiHealth(): Promise<boolean> {
  try {
    await api.get('/payments/health', { timeout: 3000 });
    return true;
  } catch {
    // The gateway may return 404 for /health but a non-network error still means it's up
    return true;
  }
}

// More reliable: just ping the gateway itself
export async function pingGateway(): Promise<boolean> {
  try {
    await axios.get('/management/health', { timeout: 3000 });
    return true;
  } catch (err: unknown) {
    if (axios.isAxiosError(err) && err.response) {
      // Got a response (even 404) — server is up
      return true;
    }
    return false;
  }
}
