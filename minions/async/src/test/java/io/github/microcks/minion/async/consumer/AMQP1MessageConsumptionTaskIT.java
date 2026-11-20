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

import io.github.microcks.domain.Header;
import io.github.microcks.domain.Secret;
import io.github.microcks.minion.async.AsyncTestSpecification;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.activemq.ArtemisContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * This is an integration test case using <a href="https://testcontainers.com/">Testcontainers</a> and an ActiveMQ
 * Artemis broker to test {@link AMQP1MessageConsumptionTask} class.
 */
@Testcontainers
class AMQP1MessageConsumptionTaskIT {

   /** The AMQP 1.0 acceptor port exposed by the Artemis container. */
   private static final int ARTEMIS_PORT = 5672;

   private static final String QUEUE_NAME = "microcks-test-queue";
   private static final String TOPIC_NAME = "microcks-test-topic";

   private static final String QUEUE_MESSAGE = "{\"message\": \"Hello from Microcks on a queue\"}";
   private static final String TOPIC_MESSAGE = "{\"message\": \"Hello from Microcks on a topic\"}";

   @Container
   private static final ArtemisContainer artemisContainer = new ArtemisContainer(
         DockerImageName.parse("apache/activemq-artemis:2.32.0-alpine"));

   @Test
   void shouldReceiveMessageOnQueueCorrectly() throws Exception {
      // Arrange.
      AsyncTestSpecification specification = buildSpecification("amqp1://%s:%d/queue/%s"
            .formatted(artemisContainer.getHost(), artemisContainer.getMappedPort(ARTEMIS_PORT), QUEUE_NAME),
            Duration.ofSeconds(3).toMillis());
      AMQP1MessageConsumptionTask task = new AMQP1MessageConsumptionTask(specification);

      // A queue retains messages, so we can publish before the consumer connects.
      publish("queue", QUEUE_NAME, QUEUE_MESSAGE);

      // Act.
      List<ConsumedMessage> messages;
      try {
         messages = task.call();
      } finally {
         task.close();
      }

      // Assert.
      assertEquals(1, messages.size());
      assertEquals(QUEUE_MESSAGE, new String(messages.get(0).getPayload(), StandardCharsets.UTF_8));

      Set<Header> headers = messages.get(0).getHeaders();
      assertNotNull(headers);
      assertEquals(1, headers.size());
      Header header = headers.iterator().next();
      assertEquals("X-Custom-Header", header.getName());
      assertEquals(Set.of("custom-value"), header.getValues());
   }

   @Test
   void shouldReceiveMessageOnTopicCorrectly() throws Exception {
      // Arrange.
      long millisToWait = Duration.ofSeconds(3).toMillis();
      AsyncTestSpecification specification = buildSpecification("amqp1://%s:%d/topic/%s".formatted(
            artemisContainer.getHost(), artemisContainer.getMappedPort(ARTEMIS_PORT), TOPIC_NAME), millisToWait + 2000);
      AMQP1MessageConsumptionTask task = new AMQP1MessageConsumptionTask(specification);

      // A topic does not retain publications: wait for the task to subscribe before publishing.
      new Thread(() -> {
         try {
            Thread.sleep(millisToWait);
            publish("topic", TOPIC_NAME, TOPIC_MESSAGE);
         } catch (Exception e) {
            throw new RuntimeException("Error while publishing message to topic", e);
         }
      }).start();

      // Act.
      List<ConsumedMessage> messages;
      try {
         messages = task.call();
      } finally {
         task.close();
      }

      // Assert.
      assertEquals(1, messages.size());
      assertEquals(TOPIC_MESSAGE, new String(messages.get(0).getPayload(), StandardCharsets.UTF_8));
   }

   private static AsyncTestSpecification buildSpecification(String endpointUrl, long timeoutMS) {
      Secret secret = new Secret();
      secret.setName("artemis-user");
      secret.setUsername(artemisContainer.getUser());
      secret.setPassword(artemisContainer.getPassword());

      AsyncTestSpecification specification = new AsyncTestSpecification();
      specification.setEndpointUrl(endpointUrl);
      specification.setTimeoutMS(timeoutMS);
      specification.setSecret(secret);
      return specification;
   }

   private static void publish(String destinationType, String destinationName, String content) throws Exception {
      ConnectionOptions connectionOptions = new ConnectionOptions();
      connectionOptions.user(artemisContainer.getUser());
      connectionOptions.password(artemisContainer.getPassword());

      SenderOptions senderOptions = new SenderOptions();
      senderOptions.targetOptions().capabilities(destinationType);

      try (Client client = Client.create();
            Connection connection = client.connect(artemisContainer.getHost(),
                  artemisContainer.getMappedPort(ARTEMIS_PORT), connectionOptions);
            Sender sender = connection.openSender(destinationName, senderOptions)) {
         Message<String> message = Message.create(content);
         message.property("X-Custom-Header", "custom-value");
         sender.send(message).awaitSettlement();
      }
   }
}
