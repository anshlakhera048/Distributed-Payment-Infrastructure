import { usePaymentStream } from '../hooks/usePaymentStream';
import { Card } from '../components/Card';
import { PaymentTable } from '../components/PaymentTable';
import { EventStream } from '../components/EventStream';
import { SystemStatusPanel } from '../components/SystemStatus';
import { AlertPanel } from '../components/AlertPanel';
import { MetricsBar } from '../components/MetricsBar';
import { CreatePaymentForm } from '../components/CreatePaymentForm';

export function Dashboard() {
  const { events, alerts, metrics, systemStatus, clearAlerts } =
    usePaymentStream();

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100">
      {/* Header */}
      <header className="border-b border-gray-800 px-6 py-4">
        <div className="max-w-[1440px] mx-auto flex items-center justify-between">
          <div>
            <h1 className="text-lg font-bold tracking-tight">
              <span className="text-cyan-400">⬡</span> Payments System
            </h1>
            <p className="text-[11px] text-gray-500 mt-0.5">
              Distributed Payment Processing — Real-Time Dashboard
            </p>
          </div>
          <div className="flex items-center gap-3">
            <span
              className={`inline-block w-2 h-2 rounded-full ${
                systemStatus.wsConnected
                  ? 'bg-emerald-400 animate-pulse'
                  : 'bg-red-400'
              }`}
            />
            <span className="text-xs text-gray-500">
              {systemStatus.wsConnected ? 'Live' : 'Disconnected'}
            </span>
          </div>
        </div>
      </header>

      <main className="max-w-[1440px] mx-auto px-6 py-6 space-y-6">
        {/* Metrics Bar */}
        <MetricsBar metrics={metrics} />

        {/* Main Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Left: Live Payment Stream (2/3 width) */}
          <Card title="Live Payment Stream" className="lg:col-span-2">
            <PaymentTable events={events} />
          </Card>

          {/* Right Sidebar */}
          <div className="space-y-6">
            <Card title="System Status">
              <SystemStatusPanel status={systemStatus} />
            </Card>

            <Card title="Create Payment">
              <CreatePaymentForm />
            </Card>
          </div>
        </div>

        {/* Bottom Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Card title="Event Stream (Kafka)">
            <EventStream events={events} />
          </Card>

          <Card title="Alerts & Fraud">
            <AlertPanel alerts={alerts} onClear={clearAlerts} />
          </Card>
        </div>
      </main>
    </div>
  );
}
