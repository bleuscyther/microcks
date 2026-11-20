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
package io.github.microcks.minion.async.consumer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This is a test case for AMQP1MessageConsumptionTask.
 */
class AMQP1MessageConsumptionTaskTest {

   @Test
   void testAcceptEndpoint() {
      assertTrue(AMQP1MessageConsumptionTask.acceptEndpoint("amqp1://localhost/queue/testQueue"));

      assertTrue(AMQP1MessageConsumptionTask.acceptEndpoint("amqp1://localhost:5672/queue/testQueue"));

      assertTrue(AMQP1MessageConsumptionTask.acceptEndpoint("amqp1://localhost:5672/topic/testTopic"));

      assertTrue(AMQP1MessageConsumptionTask.acceptEndpoint("amqp1://localhost:5672/queue/test/with/path/elements"));

      assertTrue(
            AMQP1MessageConsumptionTask.acceptEndpoint("amqp1://broker.example.com:5671/topic/testTopic?tls=true"));
   }

   @Test
   void testAcceptEndpointFailures() {
      // Destination type is mandatory and must be queue or topic.
      assertFalse(AMQP1MessageConsumptionTask.acceptEndpoint("amqp1://localhost:5672/testQueue"));

      assertFalse(AMQP1MessageConsumptionTask.acceptEndpoint("amqp1://localhost:5672/exchange/testQueue"));

      // AMQP 0.9.1 endpoints must be left to AMQPMessageConsumptionTask.
      assertFalse(AMQP1MessageConsumptionTask.acceptEndpoint("amqp://localhost:5672/q/testQueue"));

      assertFalse(AMQP1MessageConsumptionTask.acceptEndpoint(null));
   }

   @Test
   void testEndpointsDoNotOverlapWithAMQP091() {
      assertFalse(AMQPMessageConsumptionTask.acceptEndpoint("amqp1://localhost:5672/queue/testQueue"));

      assertFalse(AMQPMessageConsumptionTask.acceptEndpoint("amqp1://localhost:5672/topic/testTopic"));
   }
}
