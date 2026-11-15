package com.fireatlas.gold;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fireatlas.gold.bootstrap.controller.BootstrapController;
import com.fireatlas.gold.bootstrap.entity.BootstrapStatus;
import com.fireatlas.gold.bootstrap.service.BootstrapService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class BootstrapControllerTest {
    @Test
    void getIsPassiveAndPostReturnsAcceptedUntilReady() throws Exception {
        var service = mock(BootstrapService.class);
        var controller = new BootstrapController(service);
        var loading = new BootstrapStatus("loading", 2, 15, 1000, "Carga real", null);
        when(service.status()).thenReturn(loading);
        var passive = controller.status();
        assertEquals(200, passive.getStatusCode().value());
        assertEquals("no-store", passive.getHeaders().getCacheControl());
        verify(service, never()).start();

        when(service.start()).thenReturn(loading);
        assertEquals(202, controller.start().getStatusCode().value());
        var ready = new BootstrapStatus("ready", 15, 15, 26000000, "Pronto", null);
        when(service.start()).thenReturn(ready);
        assertEquals(200, controller.start().getStatusCode().value());

        var mapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        var json = mapper.readTree(mapper.writeValueAsString(ready));
        assertTrue(json.has("error"));
        assertTrue(json.get("error").isNull());
        assertEquals(15, json.get("completedUnits").asLong());
        assertEquals(26000000, json.get("rowsLoaded").asLong());
    }
}
