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

import io.github.microcks.domain.Header;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.activemq.ArtemisContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * This is an integration test case using <a href="https://testcontainers.com/">Testcontainers</a> and an ActiveMQ
 * Artemis broker to test {@link AMQP1ProducerManager} class.
 */
@Testcontainers
class AMQP1ProducerManagerIT {

   /** The AMQP 1.0 acceptor port exposed by the Artemis container. */
   private static final int ARTEMIS_PORT = 5672;

   @Container
   private static final ArtemisContainer artemisContainer = new ArtemisContainer(
         DockerImageName.parse("apache/activemq-artemis:2.32.0-alpine"));

   @Test
   void testPublishMessageOnQueue() throws Exception {
      // Arrange.
      AMQP1ProducerManager producerManager = buildProducerManager();

      // Act: a queue retains messages, so we can publish before any consumer connects.
      producerManager.publishMessage("queue", "test-queue", "{\"greeting\": \"hello\"}", null);

      // Assert.
      try (Client client = Client.create(); Connection connection = connect(client)) {
         ReceiverOptions options = new ReceiverOptions();
         options.sourceOptions().capabilities("queue");
         Receiver receiver = connection.openReceiver("test-queue", options);

         Delivery delivery = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull(delivery);
         assertEquals("{\"greeting\": \"hello\"}", delivery.message().body());
      } finally {
         producerManager.destroy();
      }
   }

   @Test
   void testPublishMessageWithHeadersOnTopic() throws Exception {
      // Arrange.
      AMQP1ProducerManager producerManager = buildProducerManager();

      Header header = new Header();
      header.setName("X-Custom-Header");
      header.setValues(Set.of("custom-value"));

      try (Client client = Client.create(); Connection connection = connect(client)) {
         // A topic does not retain publications: subscribe before publishing.
         ReceiverOptions options = new ReceiverOptions();
         options.sourceOptions().capabilities("topic");
         Receiver receiver = connection.openReceiver("test-topic", options);
         receiver.openFuture().get(5, TimeUnit.SECONDS);

         // Act.
         producerManager.publishMessage("topic", "test-topic", "{\"user\": \"alice\"}", Set.of(header));

         // Assert.
         Delivery delivery = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull(delivery);
         Message<Object> message = delivery.message();
         assertEquals("{\"user\": \"alice\"}", message.body());
         assertEquals("custom-value", message.property("X-Custom-Header"));
      } finally {
         producerManager.destroy();
      }
   }

   private static AMQP1ProducerManager buildProducerManager() throws Exception {
      AMQP1ProducerManager producerManager = new AMQP1ProducerManager();
      producerManager.amqpServer = artemisContainer.getHost() + ":" + artemisContainer.getMappedPort(ARTEMIS_PORT);
      producerManager.amqpClientId = "microcks-async-minion-test";
      producerManager.amqpUsername = Optional.of(artemisContainer.getUser());
      producerManager.amqpPassword = Optional.of(artemisContainer.getPassword());
      producerManager.create();
      return producerManager;
   }

   private static Connection connect(Client client) throws Exception {
      ConnectionOptions options = new ConnectionOptions();
      options.user(artemisContainer.getUser());
      options.password(artemisContainer.getPassword());
      return client.connect(artemisContainer.getHost(), artemisContainer.getMappedPort(ARTEMIS_PORT), options);
   }
}
