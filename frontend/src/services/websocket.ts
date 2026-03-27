type MessageHandler = (data: unknown) => void;

const WS_URL = 'ws://localhost:8080/ws/payments';
const RECONNECT_DELAY_MS = 3000;
const PING_INTERVAL_MS = 25000;

export class PaymentWebSocket {
  private ws: WebSocket | null = null;
  private handlers: Set<MessageHandler> = new Set();
  private statusHandlers: Set<(connected: boolean) => void> = new Set();
  private shouldReconnect = true;
  private pingTimer: ReturnType<typeof setInterval> | null = null;

  connect(): void {
    if (this.ws?.readyState === WebSocket.OPEN) return;

    this.ws = new WebSocket(WS_URL);

    this.ws.onopen = () => {
      this.notifyStatus(true);
      this.startPing();
    };

    this.ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.type === 'PONG' || data.type === 'CONNECTED') return;
        this.handlers.forEach((h) => h(data));
      } catch {
        // non-JSON message, ignore
      }
    };

    this.ws.onclose = () => {
      this.notifyStatus(false);
      this.stopPing();
      if (this.shouldReconnect) {
        setTimeout(() => this.connect(), RECONNECT_DELAY_MS);
      }
    };

    this.ws.onerror = () => {
      this.ws?.close();
    };
  }

  disconnect(): void {
    this.shouldReconnect = false;
    this.stopPing();
    this.ws?.close();
    this.ws = null;
  }

  onMessage(handler: MessageHandler): () => void {
    this.handlers.add(handler);
    return () => this.handlers.delete(handler);
  }

  onStatusChange(handler: (connected: boolean) => void): () => void {
    this.statusHandlers.add(handler);
    return () => this.statusHandlers.delete(handler);
  }

  isConnected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  private startPing(): void {
    this.pingTimer = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send('ping');
      }
    }, PING_INTERVAL_MS);
  }

  private stopPing(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }

  private notifyStatus(connected: boolean): void {
    this.statusHandlers.forEach((h) => h(connected));
  }
}

// Singleton instance
export const paymentWs = new PaymentWebSocket();
