<img src="./microcks-banner.png" width="600"> 

> ## About this fork: AMQP 1.0 support
>
> This is an unofficial fork of [microcks/microcks](https://github.com/microcks/microcks). The `feat/amqp1-support`
> branch adds **AMQP 1.0 support to the Async Minion**, next to the existing AMQP 0.9.1 (RabbitMQ) support. It is not
> part of an official Microcks release.
>
> **What was added**
>
> * Mocking: AsyncAPI operations carrying an `amqp1` binding are published to an AMQP 1.0 broker, on a queue or a topic.
> * Testing: a new `amqp1://host:port/queue|topic/name` test endpoint consumes messages from an AMQP 1.0 broker.
> * Configuration for the minion, the uber distribution, the Helm chart and the Docker / Podman Compose samples.
> * UI: AMQP 1.0 endpoint, destination and test endpoint help.
>
> Full documentation is in [minions/async/AMQP1.md](./minions/async/AMQP1.md).
>
> **What was tested**
>
> * The 94 unit tests of the Async Minion pass, including 12 new ones for AMQP 1.0.
> * 4 new integration tests pass against a real ActiveMQ Artemis 2.32.0 broker started with Testcontainers: publishing
>   to a queue, publishing to a topic with headers, consuming from a queue and consuming from a topic.
> * The code is formatted with the project `spotless` rules.
>
> **TODO: not verified yet**
>
> * [ ] Other integration tests: the existing Kafka, NATS, SQS, PubSub and IBM MQ ones were not run. One constructor
>   argument was changed in each of those five test files and they compile.
> * [ ] Webapp UI: the Angular changes were not built or viewed in a browser.
> * [ ] Native image: the native build with the new dependency was not tried.
> * [ ] Helm chart: the template changes were not rendered.
> * [ ] Brokers other than ActiveMQ Artemis (Azure Service Bus, Solace, Qpid...) were not tested.
>
> This work was prepared with the assistance of a generative AI tool (Claude Code).

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/microcks/microcks/build-verify.yml?logo=github&style=for-the-badge)](https://github.com/microcks/microcks/actions)
[![Container](https://img.shields.io/badge/dynamic/json?color=blueviolet&logo=docker&style=for-the-badge&label=Quay.io&query=tags[1].name&url=https://quay.io/api/v1/repository/microcks/microcks/tag/?limit=10&page=1&onlyActiveTags=true)](https://quay.io/repository/microcks/microcks?tab=tags)
[![Version](https://img.shields.io/maven-central/v/io.github.microcks/microcks?color=blue&style=for-the-badge)]((https://search.maven.org/artifact/io.github.microcks/microcks))
[![License](https://img.shields.io/github/license/microcks/microcks?style=for-the-badge&logo=apache)](https://www.apache.org/licenses/LICENSE-2.0)
[![Project Chat](https://img.shields.io/badge/discord-microcks-pink.svg?color=7289da&style=for-the-badge&logo=discord)](https://microcks.io/discord-invite/)
[![Artifact HUB](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/microcks&style=for-the-badge)](https://artifacthub.io/packages/search?repo=microcks)
[![CNCF Landscape](https://img.shields.io/badge/CNCF%20Landscape-5699C6?style=for-the-badge&logo=cncf)](https://landscape.cncf.io/?item=app-definition-and-development--application-definition-image-build--microcks)

# Microcks - cloud native tool for API Mocking & Testing

Microcks is a platform for turning your API and microservices assets - *OpenAPI specs*, *AsyncAPI specs*, *gRPC protobuf*, *GraphQL schema*, *Postman collections*, *SoapUI projects* - into live mocks in seconds.

It also reuses these assets to run contract conformance and non-regression tests against your API implementation. We provide integrations with *Jenkins*, *GitHub Actions*, *Tekton* and many others through a simple CLI.

[![LFX Health Score](https://img.shields.io/static/v1?label=Health%20Score&message=Healthy&color=A7F3D0&logo=linuxfoundation&logoColor=white&style=flat)](https://insights.linuxfoundation.org/project/microcks/repository/microcks-microcks) [![LFX Contributors](https://img.shields.io/static/v1?label=Contributors&message=761&color=0094FF&logo=linuxfoundation&logoColor=white&style=flat)](https://insights.linuxfoundation.org/project/microcks/repository/microcks-microcks) [![LFX Active Contributors](https://img.shields.io/static/v1?label=Active%20contributors%20(1Y)&message=208&color=0094FF&logo=linuxfoundation&logoColor=white&style=flat)](https://insights.linuxfoundation.org/project/microcks/repository/microcks-microcks)

## Getting Started

* [Documentation](https://microcks.io/documentation/tutorials/getting-started/)
* [Microcks Community](https://github.com/microcks/community) and community meeting

To get involved with our community, please familiarize yourself with the project's [Code of Conduct](./CODE_OF_CONDUCT.md).

## Build Status

The current development version is `1.16.1-SNAPSHOT`. 

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/microcks/microcks/build-verify.yml?logo=github&style=for-the-badge)](https://github.com/microcks/microcks/actions)

#### Sonarcloud Quality metrics

[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=microcks_microcks&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=microcks_microcks)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=microcks_microcks&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=microcks_microcks)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=microcks_microcks&metric=bugs)](https://sonarcloud.io/summary/new_code?id=microcks_microcks)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=microcks_microcks&metric=coverage)](https://sonarcloud.io/summary/new_code?id=microcks_microcks)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=microcks_microcks&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=microcks_microcks)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=microcks_microcks&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=microcks_microcks)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=microcks_microcks&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=microcks_microcks)

#### Fossa license and security scans

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fmicrocks%2Fmicrocks.svg?type=shield&issueType=license)](https://app.fossa.com/projects/git%2Bgithub.com%2Fmicrocks%2Fmicrocks?ref=badge_shield&issueType=license)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fmicrocks%2Fmicrocks.svg?type=shield&issueType=security)](https://app.fossa.com/projects/git%2Bgithub.com%2Fmicrocks%2Fmicrocks?ref=badge_shield&issueType=security)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fmicrocks%2Fmicrocks.svg?type=small)](https://app.fossa.com/projects/git%2Bgithub.com%2Fmicrocks%2Fmicrocks?ref=badge_small)

#### Signature, Provenance, SBOM

[![Static Badge](https://img.shields.io/badge/supply_chain-documentation-blue?logo=securityscorecard&label=Supply%20Chain&link=https%3A%2F%2Fmicrocks.io%2Fdocumentation%2Freferences%2Fcontainer-images%23software-supply-chain-security)](https://microcks.io/documentation/references/container-images#software-supply-chain-security)

#### OpenSSF best practices

[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/7513/badge)](https://bestpractices.coreinfrastructure.org/projects/7513)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/microcks/microcks/badge)](https://securityscorecards.dev/viewer/?uri=github.com/microcks/microcks)


## Versions

Here are the naming conventions we're using for current releases, ongoing development maintenance activities.

| Status      | Version           | Branch         | Container images tags |
| ----------- |-------------------|----------------|-----------------------|
| Stable      | `1.16.0`          | `release-1.16` | `1.16.0`, `latest`    |
| Dev         | `1.16.1-SNAPSHOT` | `master`       | `nightly`             |
| Maintenance | `1.14.1-SNAPSHOT` | `1.14.x`       | `maintenance`         |

Have a look at our [tested configurations](TESTED_CONFIGURATIONS.md) to know more about the versions of dependencies 
and integrations that we are using for development and testing.

## How to build Microcks

The build instructions are available in the [building guide](BUILDING.md).

## Thanks to the community!

[![Contributors](https://contrib.rocks/image?repo=microcks/microcks)](https://github.com/microcks/microcks/graphs/contributors)
[![Forkers repo roster for @microcks/microcks](http://reporoster.com/forks/microcks/microcks)](http://github.com/microcks/microcks/network/members)
