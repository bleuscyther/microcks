/*
 * Copyright The Microcks Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.microcks.minion.async.producer;

import io.github.microcks.domain.EventMessage;
import io.github.microcks.domain.Operation;
import io.github.microcks.domain.Service;
import io.github.microcks.minion.async.AsyncMockDefinition;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link AMQP1ProducerManager}.
 */
class AMQP1ProducerManagerTest {

   @Test
   void shouldResolveDefaultPort() {
      assertEquals(new AMQP1ProducerManager.ServerAddress("artemis", 5672, false),
            AMQP1ProducerManager.resolveServerAddress("artemis"));
   }

   @Test
   void shouldResolveStandardPort() {
      assertEquals(new AMQP1ProducerManager.ServerAddress("artemis", 5672, false),
            AMQP1ProducerManager.resolveServerAddress("artemis:5672"));
   }

   @Test
   void shouldResolveCustomPort() {
      assertEquals(new AMQP1ProducerManager.ServerAddress("localhost", 61616, false),
            AMQP1ProducerManager.resolveServerAddress("localhost:61616"));
   }

   @Test
   void shouldDetectTlsForTlsPort() {
      assertEquals(new AMQP1ProducerManager.ServerAddress("broker.example.com", 5671, true),
            AMQP1ProducerManager.resolveServerAddress("broker.example.com:5671"));
   }

   @Test
   void shouldHonorExplicitAmqpScheme() {
      assertEquals(new AMQP1ProducerManager.ServerAddress("broker.example.com", 5671, false),
            AMQP1ProducerManager.resolveServerAddress("amqp://broker.example.com:5671"));
   }

   @Test
   void shouldHonorExplicitAmqpsScheme() {
      assertEquals(new AMQP1ProducerManager.ServerAddress("broker.example.com", 5671, true),
            AMQP1ProducerManager.resolveServerAddress("amqps://broker.example.com"));
      assertEquals(new AMQP1ProducerManager.ServerAddress("broker.example.com", 10000, true),
            AMQP1ProducerManager.resolveServerAddress("amqps://broker.example.com:10000"));
   }

   @Test
   void shouldResolveCapabilityFromDestinationType() {
      assertEquals("topic", AMQP1ProducerManager.resolveCapability("topic"));
      assertEquals("topic", AMQP1ProducerManager.resolveCapability("Topic"));
      assertEquals("queue", AMQP1ProducerManager.resolveCapability("queue"));
      // A queue is the default when binding does not tell anything.
      assertEquals("queue", AMQP1ProducerManager.resolveCapability(null));
      assertEquals("queue", AMQP1ProducerManager.resolveCapability("unknown"));
   }

   @Test
   void shouldBuildDestinationName() {
      Service service = new Service();
      service.setName("User signed-up API");
      service.setVersion("0.1.0");

      Operation operation = new Operation();
      operation.setName("SUBSCRIBE user/signedup");

      AsyncMockDefinition definition = new AsyncMockDefinition(service, operation, List.of());

      assertEquals("UsersignedupAPI-0.1.0-user/signedup",
            new AMQP1ProducerManager().getDestinationName(definition, new EventMessage()));
   }
}
