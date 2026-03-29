import { useState } from 'react';
import { createPayment } from '../services/api';
import type { CreatePaymentRequest, PaymentResponse } from '../types';
import { StatusBadge } from './StatusBadge';

function generateUUID(): string {
  return crypto.randomUUID();
}

export function CreatePaymentForm() {
  const [form, setForm] = useState({
    userId: '',
    merchantId: '',
    amount: '',
    currency: 'USD',
    description: '',
    idempotencyKey: generateUUID(),
  });
  const [response, setResponse] = useState<PaymentResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setResponse(null);
    setLoading(true);

    try {
      const request: CreatePaymentRequest = {
        userId: form.userId || generateUUID(),
        merchantId: form.merchantId || generateUUID(),
        amount: parseFloat(form.amount),
        currency: form.currency,
        description: form.description || undefined,
      };
      const res = await createPayment(request, form.idempotencyKey);
      setResponse(res);
      // Reset idempotency key for next payment
      setForm((prev) => ({ ...prev, idempotencyKey: generateUUID() }));
    } catch (err: unknown) {
      if (err instanceof Error) {
        setError(err.message);
      } else {
        setError('Payment request failed');
      }
    } finally {
      setLoading(false);
    }
  };

  const handleDuplicate = async () => {
    // Re-send with the SAME idempotency key to test idempotency.
    // Uses the last successful response's userId/merchantId to ensure
    // the duplicate payload matches exactly (idempotency key is the dedup mechanism).
    setError(null);
    setLoading(true);
    try {
      const request: CreatePaymentRequest = {
        userId: response?.userId || form.userId || generateUUID(),
        merchantId: response?.merchantId || form.merchantId || generateUUID(),
        amount: parseFloat(form.amount) || 100,
        currency: form.currency,
      };
      const key = form.idempotencyKey;
      const res = await createPayment(request, key);
      setResponse(res);
    } catch (err: unknown) {
      if (err instanceof Error) {
        setError(`Duplicate test: ${err.message}`);
      } else {
        setError('Duplicate request failed');
      }
    } finally {
      setLoading(false);
    }
  };

  const handleBurst = async () => {
    setError(null);
    setResponse(null);
    setLoading(true);
    try {
      const requests = Array.from({ length: 5 }, () => {
        const request: CreatePaymentRequest = {
          userId: generateUUID(),
          merchantId: generateUUID(),
          amount: Math.floor(Math.random() * 9900 + 100),
          currency: 'USD',
        };
        return createPayment(request, generateUUID());
      });
      const results = await Promise.allSettled(requests);
      const succeeded = results.filter((r) => r.status === 'fulfilled').length;
      const failed = results.filter((r) => r.status === 'rejected').length;
      setError(`Burst: ${succeeded} succeeded, ${failed} failed`);
    } catch {
      setError('Burst request failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      <form onSubmit={handleSubmit} className="space-y-3">
        <div className="grid grid-cols-2 gap-3">
          <Input
            label="User ID"
            placeholder="auto-generated if empty"
            value={form.userId}
            onChange={(v) => setForm((p) => ({ ...p, userId: v }))}
          />
          <Input
            label="Merchant ID"
            placeholder="auto-generated if empty"
            value={form.merchantId}
            onChange={(v) => setForm((p) => ({ ...p, merchantId: v }))}
          />
        </div>
        <div className="grid grid-cols-2 gap-3">
          <Input
            label="Amount"
            type="number"
            placeholder="100.00"
            value={form.amount}
            onChange={(v) => setForm((p) => ({ ...p, amount: v }))}
            required
          />
          <div>
            <label className="block text-[10px] uppercase tracking-wider text-gray-500 mb-1">
              Currency
            </label>
            <select
              value={form.currency}
              onChange={(e) =>
                setForm((p) => ({ ...p, currency: e.target.value }))
              }
              className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm text-gray-200 focus:border-cyan-500 focus:outline-none"
            >
              {['USD', 'EUR', 'GBP', 'INR', 'JPY', 'CAD', 'AUD', 'XYZ'].map(
                (c) => (
                  <option key={c} value={c}>
                    {c} {c === 'XYZ' ? '(triggers fraud)' : ''}
                  </option>
                )
              )}
            </select>
          </div>
        </div>
        <Input
          label="Idempotency Key"
          value={form.idempotencyKey}
          onChange={(v) => setForm((p) => ({ ...p, idempotencyKey: v }))}
          required
        />
        <Input
          label="Description"
          placeholder="Optional"
          value={form.description}
          onChange={(v) => setForm((p) => ({ ...p, description: v }))}
        />

        <div className="flex gap-2 pt-1">
          <button
            type="submit"
            disabled={loading || !form.amount}
            className="flex-1 bg-cyan-600 hover:bg-cyan-500 disabled:bg-gray-700 disabled:text-gray-500 text-white text-sm font-medium py-2 px-4 rounded transition-colors"
          >
            {loading ? 'Sending…' : 'Create Payment'}
          </button>
        </div>
      </form>

      {/* Simulation Controls */}
      <div className="flex gap-2 mt-3 pt-3 border-t border-gray-800">
        <button
          onClick={handleDuplicate}
          disabled={loading}
          className="text-xs bg-yellow-600/20 text-yellow-400 hover:bg-yellow-600/30 px-3 py-1.5 rounded transition-colors"
        >
          ↻ Duplicate Request
        </button>
        <button
          onClick={handleBurst}
          disabled={loading}
          className="text-xs bg-orange-600/20 text-orange-400 hover:bg-orange-600/30 px-3 py-1.5 rounded transition-colors"
        >
          ⚡ Burst (5 rapid)
        </button>
      </div>

      {/* Response / Error display */}
      {error && (
        <div className="mt-3 p-3 rounded bg-red-500/10 border border-red-500/30 text-red-300 text-xs">
          {error}
        </div>
      )}
      {response && (
        <div className="mt-3 p-3 rounded bg-gray-800/80 border border-gray-700 text-xs space-y-1">
          <div className="flex items-center justify-between">
            <span className="text-gray-400">Response</span>
            <StatusBadge status={response.status} />
          </div>
          <div className="font-mono text-gray-300 break-all">
            ID: {response.paymentId}
          </div>
          <div className="text-gray-500">
            {response.amount} {response.currency}
          </div>
        </div>
      )}
    </div>
  );
}

function Input({
  label,
  value,
  onChange,
  placeholder,
  type = 'text',
  required = false,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  type?: string;
  required?: boolean;
}) {
  return (
    <div>
      <label className="block text-[10px] uppercase tracking-wider text-gray-500 mb-1">
        {label}
      </label>
      <input
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        required={required}
        step={type === 'number' ? '0.01' : undefined}
        min={type === 'number' ? '0.01' : undefined}
        className="w-full bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm text-gray-200 placeholder-gray-600 focus:border-cyan-500 focus:outline-none"
      />
    </div>
  );
}
