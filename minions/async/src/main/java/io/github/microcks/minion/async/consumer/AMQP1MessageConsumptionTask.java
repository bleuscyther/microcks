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
import io.github.microcks.minion.async.AsyncTestSpecification;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.jboss.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An implementation of <code>MessageConsumptionTask</code> that consumes a queue or a topic on an AMQP 1.0 broker (ie.
 * ActiveMQ Artemis and other compliant brokers). Endpoint URL should be specified using the following form:
 * <code>amqp1://{brokerhost[:port]}/{type}/{destination}[?option1=value1&amp;option2=value2]</code> where
 * <code>type</code> is either <code>queue</code> (point-to-point consumption) or <code>topic</code> (publish/subscribe
 * consumption through a non-durable subscription).
 */
public class AMQP1MessageConsumptionTask implements MessageConsumptionTask {

   /** Get a JBoss logging logger. */
   private final Logger logger = Logger.getLogger(getClass());

   /** The string for Regular Expression that helps validating acceptable endpoints. */
   public static final String ENDPOINT_PATTERN_STRING = "amqp1://(?<brokerUrl>[^:/]+(:\\d+)?)/(?<type>queue|topic)/(?<destination>[^?]+)(\\?(?<options>.+))?";
   /** The Pattern for matching groups within the endpoint regular expression. */
   public static final Pattern ENDPOINT_PATTERN = Pattern.compile(ENDPOINT_PATTERN_STRING);

   /** Constant representing a queue destination type in endpoint URL. */
   public static final String QUEUE_TYPE = "queue";
   /** Constant representing a topic destination type in endpoint URL. */
   public static final String TOPIC_TYPE = "topic";

   /** The endpoint URL option forcing the usage of a TLS secured connection. */
   public static final String TLS_OPTION = "tls";

   /** The default port for plain AMQP connections. */
   private static final int DEFAULT_PORT = 5672;
   /** The default port for TLS secured AMQP connections. */
   private static final int DEFAULT_TLS_PORT = 5671;

   private File trustStore;

   private final AsyncTestSpecification specification;

   protected Map<String, String> optionsMap;

   private Client client;

   private Connection connection;

   private String destinationType;

   private String destinationName;

   /**
    * Create a new consumption task from an Async test specification.
    * @param testSpecification The specification holding endpointURL and timeout.
    */
   public AMQP1MessageConsumptionTask(AsyncTestSpecification testSpecification) {
      this.specification = testSpecification;
   }

   /**
    * Convenient static method for checking if this implementation will accept endpoint.
    * @param endpointUrl The endpoint URL to validate
    * @return True if endpointUrl can be used for connecting and consuming on endpoint
    */
   public static boolean acceptEndpoint(String endpointUrl) {
      return endpointUrl != null && endpointUrl.matches(ENDPOINT_PATTERN_STRING);
   }

   @Override
   public List<ConsumedMessage> call() throws Exception {
      if (connection == null) {
         initializeAMQPConnection();
      }
      List<ConsumedMessage> messages = new ArrayList<>();

      // AMQP 1.0 has no protocol-level notion of queue or topic: advertise the expected kind of destination to the
      // broker using the well-known terminus capabilities.
      ReceiverOptions receiverOptions = new ReceiverOptions();
      receiverOptions.sourceOptions().capabilities(destinationType);

      long timeoutTime = System.currentTimeMillis() + specification.getTimeoutMS();

      try (Receiver receiver = connection.openReceiver(destinationName, receiverOptions)) {
         long remainingTime = specification.getTimeoutMS();

         // Poll messages until timeout, only waiting for the remaining time so that we never overshoot it.
         while (remainingTime > 0) {
            Delivery delivery = receiver.receive(remainingTime, TimeUnit.MILLISECONDS);
            if (delivery != null) {
               Message<Object> amqpMessage = delivery.message();
               byte[] payload = extractPayload(amqpMessage.body());
               logger.infof("Received a new AMQP 1.0 Message: %s", new String(payload, StandardCharsets.UTF_8));

               // Build a ConsumedMessage from AMQP message.
               ConsumedMessage message = new ConsumedMessage();
               message.setReceivedAt(System.currentTimeMillis());
               message.setHeaders(buildHeaders(amqpMessage));
               message.setPayload(payload);
               messages.add(message);
            }
            remainingTime = timeoutTime - System.currentTimeMillis();
         }
      }

      return messages;
   }

   /**
    * Close the resources used by this task. Namely the AMQP connection and the optionally created truststore holding
    * server client SSL credentials.
    * @throws IOException should not happen.
    */
   @Override
   public void close() throws IOException {
      if (connection != null) {
         connection.close();
      }
      if (client != null) {
         client.close();
      }
      if (trustStore != null && trustStore.exists()) {
         Files.delete(trustStore.toPath());
      }
   }

   /** Initialize AMQP 1.0 connection from test properties. */
   private void initializeAMQPConnection() throws Exception {
      Matcher matcher = ENDPOINT_PATTERN.matcher(specification.getEndpointUrl().trim());
      // Call matcher.find() to be able to use named expressions.
      matcher.find();
      String endpointBrokerUrl = matcher.group("brokerUrl");
      destinationType = matcher.group("type");
      destinationName = matcher.group("destination");
      String options = matcher.group("options");

      // Parse options if specified.
      if (options != null && !options.isBlank()) {
         optionsMap = ConsumptionTaskCommons.initializeOptionsMap(options);
      }

      String host = endpointBrokerUrl;
      Integer port = null;
      if (endpointBrokerUrl.contains(":")) {
         String[] serverAndPort = endpointBrokerUrl.split(":");
         host = serverAndPort[0];
         port = Integer.parseInt(serverAndPort[1]);
      }

      // TLS may be explicitly required with an option, otherwise it is deduced from the standard secured port.
      boolean tls = port != null && port == DEFAULT_TLS_PORT;
      if (optionsMap != null && optionsMap.containsKey(TLS_OPTION)) {
         tls = Boolean.parseBoolean(optionsMap.get(TLS_OPTION));
      }

      ConnectionOptions connectionOptions = new ConnectionOptions();

      if (specification.getSecret() != null) {
         if (specification.getSecret().getUsername() != null && specification.getSecret().getPassword() != null) {
            logger.debug("Adding username/password authentication from secret " + specification.getSecret().getName());
            connectionOptions.user(specification.getSecret().getUsername());
            connectionOptions.password(specification.getSecret().getPassword());
         }

         if (specification.getSecret().getCaCertPem() != null) {
            logger.debug("Installing a broker certificate from secret " + specification.getSecret().getName());
            trustStore = ConsumptionTaskCommons.installBrokerCertificate(specification);

            // A custom broker certificate only makes sense on a secured connection.
            tls = true;
            connectionOptions.sslOptions().trustStoreLocation(trustStore.getAbsolutePath())
                  .trustStorePassword(ConsumptionTaskCommons.TRUSTSTORE_PASSWORD);
         }
      }
      connectionOptions.sslEnabled(tls);

      if (port == null) {
         port = tls ? DEFAULT_TLS_PORT : DEFAULT_PORT;
      }

      client = Client.create();
      connection = client.connect(host, port, connectionOptions);
   }

   /**
    * Extract a bytes payload from the body of an AMQP 1.0 message. Depending on the publisher, body may come as a
    * binary data section, as a string value or as any other AMQP typed value.
    */
   private byte[] extractPayload(Object body) {
      if (body == null) {
         return new byte[0];
      }
      if (body instanceof byte[] bytes) {
         return bytes;
      }
      return body.toString().getBytes(StandardCharsets.UTF_8);
   }

   /** Build set of Microcks headers from AMQP 1.0 message application properties. */
   private Set<Header> buildHeaders(Message<Object> amqpMessage) throws ClientException {
      if (!amqpMessage.hasProperties()) {
         return null;
      }
      Set<Header> results = new HashSet<>();
      amqpMessage.forEachProperty((name, value) -> {
         Header result = new Header();
         result.setName(name);
         result.setValues(Set.of(String.valueOf(value)));
         results.add(result);
      });
      return results;
   }
}
