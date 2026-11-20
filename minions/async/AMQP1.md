# AMQP 1.0 support in the Async Minion

The Async Minion can mock and test [AsyncAPI](https://www.asyncapi.com) operations over **AMQP 1.0**, the ISO/OASIS
standard implemented by ActiveMQ Artemis, Azure Service Bus, Solace, Qpid and others.

AMQP 1.0 is a different wire protocol from AMQP 0.9.1 (RabbitMQ). Microcks treats them as two separate bindings:

| Binding  | Protocol   | Typical broker   | Configuration prefix | Test endpoint scheme |
| -------- | ---------- | ---------------- | -------------------- | -------------------- |
| `AMQP`   | AMQP 0.9.1 | RabbitMQ         | `amqp.`              | `amqp://`            |
| `AMQP1`  | AMQP 1.0   | ActiveMQ Artemis | `amqp1.`             | `amqp1://`           |

The implementation relies on the [Apache Qpid ProtonJ2](https://qpid.apache.org/proton/) client and has been tested
against ActiveMQ Artemis only. See [Limitations](#limitations).

## Declaring the binding in an AsyncAPI document

An operation is mocked over AMQP 1.0 when it carries an `amqp1` operation binding. Microcks reads one optional
property from it:

| Property          | Values             | Default | Meaning                                               |
| ----------------- | ------------------ | ------- | ----------------------------------------------------- |
| `destinationType` | `queue` or `topic` | `queue` | Point-to-point queue or publish/subscribe topic.      |

```yaml
asyncapi: '2.6.0'
info:
  title: User signed-up API
  version: 0.1.0
channels:
  user/signedup:
    subscribe:
      bindings:
        amqp1:
          destinationType: topic
      message:
        $ref: '#/components/messages/UserSignedUp'
```

> The AsyncAPI `amqp1` binding specification does not define any field yet, so `destinationType` is a Microcks
> convention and not part of the standard.

## Enabling the binding

The minion only handles the bindings listed in `minion.supported-bindings`, so `AMQP1` must be added to it together
with the connection properties:

```properties
minion.supported-bindings=KAFKA,WS,AMQP1

amqp1.server=localhost:5672
amqp1.username=microcks
amqp1.password=microcks
```

| Property         | Required | Default                 | Description                                              |
| ---------------- | -------- | ----------------------- | -------------------------------------------------------- |
| `amqp1.server`   | yes      | `localhost:5672`        | Broker address, see the accepted forms below.            |
| `amqp1.username` | no       | none                    | Username. Anonymous connection if username or password is empty. |
| `amqp1.password` | no       | none                    | Password.                                                |
| `amqp1.clientid` | no       | `microcks-async-minion` | AMQP container id presented to the broker.               |

`amqp1.server` accepts the following forms:

| Value                              | Host                 | Port  | TLS |
| ---------------------------------- | -------------------- | ----- | --- |
| `artemis`                          | `artemis`            | 5672  | no  |
| `artemis:5672`                     | `artemis`            | 5672  | no  |
| `broker.example.com:5671`          | `broker.example.com` | 5671  | yes |
| `amqps://broker.example.com`       | `broker.example.com` | 5671  | yes |
| `amqps://broker.example.com:10000` | `broker.example.com` | 10000 | yes |
| `amqp://broker.example.com:5671`   | `broker.example.com` | 5671  | no  |

An explicit `amqp://` or `amqps://` scheme always wins. Without a scheme, TLS is enabled only for port `5671`. The
connection reconnects automatically if the broker goes away.

### Helm chart

```yaml
features:
  async:
    enabled: true
    amqp1:
      url: artemis:5672
      username: microcks
      password: microcks
```

Setting `features.async.amqp1.url` adds `AMQP1` to the supported bindings and shows the endpoint in the Microcks UI.

### Docker Compose and Podman Compose

Uncomment and adapt the `amqp1` lines in `config/application.properties`, and add `AMQP1` to
`minion.supported-bindings`. Set `features.feature.async-api.endpoint-AMQP1` in `config/features.properties` so
that the UI displays the broker address.

### Uber Async Minion

The uber distribution is configured through environment variables:

| Variable          | Example        |
| ----------------- | -------------- |
| `ASYNC_PROTOCOLS` | `,KAFKA,AMQP1` |
| `AMQP1_SERVER`    | `artemis:5672` |
| `AMQP1_USERNAME`  | `microcks`     |
| `AMQP1_PASSWORD`  | `microcks`     |

## How mock messages are published

**Destination name.** Messages are sent to the address `{service}-{version}-{operation}` where:

- `{service}` is the API name with spaces and `-` removed,
- `{version}` is the API version with spaces removed,
- `{operation}` is the channel name, with path parameters replaced by the example values.

For the document above, the address is `UsersignedupAPI-0.1.0-user/signedup`. The Microcks UI shows the exact
address of each example message.

**Queue or topic.** AMQP 1.0 has no protocol-level notion of queue or topic. The minion advertises the binding's
`destinationType` to the broker as a `queue` or `topic` terminus capability. ActiveMQ Artemis uses it to create the
address on the fly with the matching routing type (anycast for a queue, multicast for a topic). Brokers that do not
understand these capabilities need the destination to be provisioned beforehand.

**Message format.**

| Microcks example  | AMQP 1.0 message                                      |
| ----------------- | ----------------------------------------------------- |
| Payload           | Body, as an AMQP string value (after template rendering). |
| Headers           | Application properties, first value of each header.   |

## Testing an event producer

To check that an application publishes messages conforming to the AsyncAPI document, launch a test with an endpoint
of the following form:

```
amqp1://{host}[:{port}]/{queue|topic}/{destination}[?tls=true]
```

Examples:

```
amqp1://artemis.example.com:5672/queue/orders
amqp1://artemis.example.com:5672/topic/user/signedup
amqp1://broker.example.com:5671/topic/user/signedup?tls=true
```

- `queue` consumes the messages waiting on the queue.
- `topic` opens a non-durable subscription: only messages published while the test is running are received.
- The port defaults to `5672`, or `5671` when TLS is enabled.
- TLS is enabled on port `5671`, with `?tls=true`, or when the test Secret holds a CA certificate.
- Credentials come from the Secret attached to the test (username and password). The Secret CA certificate, if any,
  is used as the trusted certificate for the broker.

Received application properties are exposed as message headers. Binary and string bodies are both supported.

## Trying it locally

Start an Artemis broker, which accepts AMQP 1.0 on port `5672` and uses `artemis` / `artemis` as default
credentials:

```sh
docker run --rm -p 5672:5672 -p 8161:8161 apache/activemq-artemis:2.32.0-alpine
```

Then point the minion at it using the properties described in [Enabling the binding](#enabling-the-binding), with
`amqp1.server=localhost:5672`, `amqp1.username=artemis` and `amqp1.password=artemis`.

## Running the tests

Unit tests:

```sh
mvn -pl minions/async test
```

Integration tests use [Testcontainers](https://testcontainers.com) to start an Artemis broker and need a running
Docker daemon:

```sh
mvn -pl minions/async test -Dtest='AMQP1*IT' -Dsurefire.failIfNoSpecifiedTests=false
```

## Limitations

- Only ActiveMQ Artemis has been tested. Other AMQP 1.0 brokers may ignore the `queue` / `topic` capabilities.
- Request-reply operations are not handled over AMQP 1.0.
- Avro binary payloads are not handled: the payload is always published as a string.
- The mock publisher has no option for a custom trust store: the broker certificate must be trusted by the JVM.
- The native image build has not been validated with the ProtonJ2 client.
