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
import io.github.microcks.domain.Header;
import io.github.microcks.minion.async.AsyncMockDefinition;
import io.github.microcks.util.el.TemplateEngine;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.ClientOptions;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * AMQP 1.0 (ie. ActiveMQ Artemis and other compliant brokers) implementation of producer for async event messages.
 */
@ApplicationScoped
public class AMQP1ProducerManager {

   /** Get a JBoss logging logger. */
   private final Logger logger = Logger.getLogger(getClass());

   /** Destination type used in AsyncAPI AMQP 1.0 binding to represent a publish/subscribe topic. */
   public static final String TOPIC_DESTINATION_TYPE = "topic";
   /** Destination type used in AsyncAPI AMQP 1.0 binding to represent a point-to-point queue. */
   public static final String QUEUE_DESTINATION_TYPE = "queue";

   /** The default port for plain AMQP connections. */
   static final int DEFAULT_PORT = 5672;
   /** The default port for TLS secured AMQP connections. */
   static final int DEFAULT_TLS_PORT = 5671;

   private static final String AMQP_SCHEME = "amqp://";
   private static final String AMQPS_SCHEME = "amqps://";

   private Client amqpClient;
   private Connection amqpConnection;

   @ConfigProperty(name = "amqp1.server")
   String amqpServer;

   @ConfigProperty(name = "amqp1.clientid", defaultValue = "microcks-async-minion")
   String amqpClientId;

   @ConfigProperty(name = "amqp1.username")
   Optional<String> amqpUsername;

   @ConfigProperty(name = "amqp1.password")
   Optional<String> amqpPassword;

   /**
    * The address of an AMQP 1.0 broker resolved from configuration.
    * @param host The broker hostname
    * @param port The broker port
    * @param tls  Whether the connection must be secured using TLS
    */
   record ServerAddress(String host, int port, boolean tls) {
   }

   /**
    * Initialize the AMQP 1.0 connection post construction.
    * @throws Exception If connection to AMQP 1.0 Broker cannot be done.
    */
   @PostConstruct
   public void create() throws Exception {
      try {
         amqpConnection = createConnection();
      } catch (Exception e) {
         logger.errorf("Cannot connect to AMQP 1.0 broker %s", amqpServer);
         logger.errorf("Connection exception: %s", e.getMessage());
         throw e;
      }
   }

   /** Release the AMQP 1.0 connection and client before destruction. */
   @PreDestroy
   public void destroy() {
      if (amqpConnection != null) {
         amqpConnection.close();
      }
      if (amqpClient != null) {
         amqpClient.close();
      }
   }

   /**
    * @return A newly created connection to configured broker
    * @throws Exception in case of connection failure
    */
   protected Connection createConnection() throws Exception {
      ServerAddress address = resolveServerAddress(amqpServer);

      ConnectionOptions options = new ConnectionOptions();
      options.reconnectEnabled(true);
      options.sslEnabled(address.tls());

      if (amqpUsername.isPresent() && !amqpUsername.get().isEmpty() && amqpPassword.isPresent()
            && !amqpPassword.get().isEmpty()) {
         logger.infof("Connecting to AMQP 1.0 broker with user '%s'", amqpUsername.get());
         options.user(amqpUsername.get());
         options.password(amqpPassword.get());
      }

      amqpClient = Client.create(new ClientOptions().id(amqpClientId));
      return amqpClient.connect(address.host(), address.port(), options);
   }

   /**
    * Resolve the AMQP 1.0 broker address from a configured server value. Honors explicit {@code amqp://} or
    * {@code amqps://} schemes; auto-detects TLS for port 5671.
    * @param server The configured AMQP 1.0 server address
    * @return The host, port and TLS flag to use for connecting
    */
   static ServerAddress resolveServerAddress(String server) {
      String hostAndPort = server.trim();
      Boolean tls = null;

      if (hostAndPort.startsWith(AMQPS_SCHEME)) {
         tls = true;
         hostAndPort = hostAndPort.substring(AMQPS_SCHEME.length());
      } else if (hostAndPort.startsWith(AMQP_SCHEME)) {
         tls = false;
         hostAndPort = hostAndPort.substring(AMQP_SCHEME.length());
      }

      String host = hostAndPort;
      Integer port = null;
      if (hostAndPort.contains(":")) {
         String[] parts = hostAndPort.split(":");
         host = parts[0];
         port = Integer.parseInt(parts[1]);
      }

      if (tls == null) {
         tls = port != null && port == DEFAULT_TLS_PORT;
      }
      if (port == null) {
         port = tls ? DEFAULT_TLS_PORT : DEFAULT_PORT;
      }
      return new ServerAddress(host, port, tls);
   }

   /**
    * Publish a message on specified destination. AMQP 1.0 has no protocol-level notion of queue or topic: we advertise
    * the expected kind of destination to the broker using the well-known <code>queue</code> and <code>topic</code>
    * terminus capabilities, so that brokers creating addresses on the fly can pick the right routing semantic.
    * @param destinationType The type of destination ('queue' or 'topic'); defaults to a queue when null or unknown
    * @param destinationName The name of destination (the AMQP 1.0 address)
    * @param value           The message payload
    * @param headers         A set of headers if any (maybe null or empty)
    */
   public void publishMessage(String destinationType, String destinationName, String value, Set<Header> headers) {
      logger.infof("Publishing on destination {%s}, message: %s ", destinationName, value);

      SenderOptions senderOptions = new SenderOptions();
      senderOptions.targetOptions().capabilities(resolveCapability(destinationType));

      try (Sender sender = amqpConnection.openSender(destinationName, senderOptions)) {
         Message<String> message = Message.create(value);

         // Adding headers as application properties if provided.
         if (headers != null && !headers.isEmpty()) {
            for (Header header : headers) {
               message.property(header.getName(), header.getValues().toArray()[0]);
            }
         }
         sender.send(message);
      } catch (ClientException ce) {
         logger.warnf("Message %s sending has thrown an exception", ce);
      }
   }

   /**
    * Get the terminus capability to advertise for a destination type.
    * @param destinationType The type of destination coming from the binding definition (maybe null)
    * @return 'topic' if destination is a topic, 'queue' in all other cases
    */
   static String resolveCapability(String destinationType) {
      return TOPIC_DESTINATION_TYPE.equalsIgnoreCase(destinationType) ? TOPIC_DESTINATION_TYPE : QUEUE_DESTINATION_TYPE;
   }

   /**
    * Get the AMQP 1.0 destination name (the address) corresponding to a AsyncMockDefinition, sanitizing all parameters.
    * @param definition   The AsyncMockDefinition
    * @param eventMessage The message to get destination name
    * @return The destination name for definition and event
    */
   public String getDestinationName(AsyncMockDefinition definition, EventMessage eventMessage) {
      // Produce service name part of destination name.
      String serviceName = definition.getOwnerService().getName().replace(" ", "");
      serviceName = serviceName.replace("-", "");

      // Produce version name part of destination name.
      String versionName = definition.getOwnerService().getVersion().replace(" ", "");

      // Produce operation name part of destination name.
      String operationName = ProducerManager.getDestinationOperationPart(definition.getOperation(), eventMessage);

      // Aggregate the 3 parts using '-' as delimiter.
      return serviceName + "-" + versionName + "-" + operationName;
   }

   /**
    * Render Microcks headers using the template engine.
    * @param engine  The template engine to reuse (because we do not want to initialize and manage a context at the
    *                AMQP1ProducerManager level.)
    * @param headers The Microcks event message headers definition.
    * @return A set of rendered Microcks headers.
    */
   public Set<Header> renderEventMessageHeaders(TemplateEngine engine, Set<Header> headers) {
      if (headers != null && !headers.isEmpty()) {
         Set<Header> renderedHeaders = HashSet.newHashSet(headers.size());

         for (Header header : headers) {
            Optional<String> optionalValue = header.getValues().stream().findFirst();
            if (optionalValue.isPresent()) {
               String firstValue = optionalValue.get();
               String renderedValue = firstValue;
               if (firstValue.contains(TemplateEngine.DEFAULT_EXPRESSION_PREFIX)) {
                  try {
                     renderedValue = engine.getValue(firstValue);
                  } catch (Exception e) {
                     logger.error("Failing at evaluating template " + firstValue, e);
                  }
               }
               Header renderedHeader = new Header();
               renderedHeader.setName(header.getName());
               renderedHeader.setValues(Set.of(renderedValue));
               renderedHeaders.add(renderedHeader);
            }
         }
         return renderedHeaders;
      }

      return Collections.emptySet();
   }
}
