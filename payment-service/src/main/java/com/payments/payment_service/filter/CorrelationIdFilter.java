package com.payments.payment_service.filter;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

/**
 * Servlet filter that initialises distributed tracing context for every inbound request.
 *
 * TWO DISTINCT IDs:
 *
 * 1. traceId (X-Trace-ID)
 *    The root identifier for an entire distributed operation. Generated ONCE at the API
 *    entry point (here, or at the API Gateway) and propagated unchanged through:
 *      HTTP headers → MDC → PaymentEvent.traceId → Kafka headers (x-trace-id) → MDC in consumers
 *    All log lines across all services share the same traceId for the same user operation.
 *    Use this to reconstruct the full end-to-end trace in Grafana/ELK/Jaeger.
 *
 * 2. correlationId (X-Correlation-ID)
 *    A per-request/per-hop identifier. May be supplied by the caller (e.g. API Gateway
 *    injects X-Correlation-ID before routing). Used to correlate log entries within a
 *    single service call. Re-generated at each hop if not supplied by the upstream.
 *
 * Both are added to MDC and reflected in response headers so callers can observe them.
 */
@Component
@Order(1)
public class CorrelationIdFilter implements Filter {

    private static final String TRACE_ID_HEADER       = "X-Trace-ID";
    private static final String CORRELATION_ID_HEADER = "X-Correlation-ID";
    private static final String MDC_TRACE_ID          = "traceId";
    private static final String MDC_CORRELATION_ID    = "correlationId";
    private static final String MDC_REQUEST_ID        = "requestId";
    private static final String MDC_SERVICE_NAME      = "serviceName";
    private static final String SERVICE_NAME          = "payment-service";

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {

        HttpServletRequest  httpRequest  = (HttpServletRequest)  request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        // traceId: accept from upstream (API Gateway sets it), otherwise generate root trace
        String traceId = httpRequest.getHeader(TRACE_ID_HEADER);
        if (traceId == null || traceId.isBlank()) {
            traceId = UUID.randomUUID().toString();
        }

        // correlationId: accept from upstream or generate for this hop
        String correlationId = httpRequest.getHeader(CORRELATION_ID_HEADER);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        // requestId: unique per HTTP request (never propagated by callers)
        String requestId = UUID.randomUUID().toString();

        MDC.put(MDC_TRACE_ID,       traceId);
        MDC.put(MDC_CORRELATION_ID, correlationId);
        MDC.put(MDC_REQUEST_ID,     requestId);
        MDC.put(MDC_SERVICE_NAME,   SERVICE_NAME);

        httpResponse.setHeader(TRACE_ID_HEADER,       traceId);
        httpResponse.setHeader(CORRELATION_ID_HEADER, correlationId);

        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(MDC_TRACE_ID);
            MDC.remove(MDC_CORRELATION_ID);
            MDC.remove(MDC_REQUEST_ID);
            MDC.remove(MDC_SERVICE_NAME);
        }
    }
}

