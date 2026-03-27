package com.payments.payment_service.controller;

import com.payments.payment_service.dto.RegisterWebhookRequest;
import com.payments.payment_service.entity.WebhookEndpoint;
import com.payments.payment_service.repository.WebhookEndpointRepository;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/webhooks")
public class WebhookController {

    private final WebhookEndpointRepository webhookEndpointRepository;

    public WebhookController(WebhookEndpointRepository webhookEndpointRepository) {
        this.webhookEndpointRepository = webhookEndpointRepository;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> registerWebhook(
        @Valid @RequestBody RegisterWebhookRequest request
    ) {
        WebhookEndpoint endpoint = new WebhookEndpoint();
        endpoint.setId(UUID.randomUUID());
        endpoint.setMerchantId(request.getMerchantId());
        endpoint.setUrl(request.getUrl());
        endpoint.setEvents(request.getEvents());
        // Generate a random signing secret — merchant should store this securely
        endpoint.setSecret(UUID.randomUUID().toString().replace("-", ""));

        webhookEndpointRepository.save(endpoint);

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
            "id", endpoint.getId(),
            "url", endpoint.getUrl(),
            "events", endpoint.getEvents(),
            "secret", endpoint.getSecret(),
            "active", endpoint.isActive()
        ));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deactivateWebhook(@PathVariable UUID id) {
        webhookEndpointRepository.findById(id).ifPresent(endpoint -> {
            endpoint.setActive(false);
            webhookEndpointRepository.save(endpoint);
        });
        return ResponseEntity.noContent().build();
    }
}
