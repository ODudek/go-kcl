# Go KCL

![technology Go](https://img.shields.io/badge/technology-go-blue.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/ODudek/go-kcl)](https://goreportcard.com/report/github.com/ODudek/go-kcl)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![go-kcl](https://github.com/ODudek/go-kcl/actions/workflows/ci.yml/badge.svg)](https://github.com/ODudek/go-kcl/actions/workflows/ci.yml)

## Overview

Go-KCL is a native open-source Go library for Amazon Kinesis Data Stream (KDS) consumption. It allows developers
to program KDS consumers in lightweight Go language and still take advantage of the features presented by the native
KDS Java API libraries.

[go-kcl](https://github.com/ODudek/go-kcl) is a fork of the original [vmware-go-kcl-v2](https://github.com/vmware/vmware-go-kcl-v2), which is no longer actively maintained. This project uses [AWS Go SDK V2](https://github.com/aws/aws-sdk-go-v2).

## Try it out

### Prerequisites

* [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2)
* The v2 SDK requires a minimum version of `Go 1.21`.
* [gosec](https://github.com/securego/gosec)

### Build & Run

1. Initialize Project

2. Build
    > `make build`

3. Test
    > `make test`

## Documentation

Go-KCL matches exactly the same interface and programming model from original Amazon KCL, the best place for getting reference, tutorial is from Amazon itself:

* [Developing Consumers Using the Kinesis Client Library](https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html)
* [Troubleshooting](https://docs.aws.amazon.com/streams/latest/dev/troubleshooting-consumers.html)
* [Advanced Topics](https://docs.aws.amazon.com/streams/latest/dev/advanced-consumers.html)

## Contributing

The go-kcl project team welcomes contributions from the community. Before you start working with go-kcl, please
read our [Developer Certificate of Origin](https://cla.vmware.com/dco). All contributions to this repository must be
signed as described on that page. Your signature certifies that you wrote the patch or have the right to pass it on
as an open-source patch. For more detailed information, refer to [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT License
