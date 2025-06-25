# Workflow Overview

The file gives an overview on what kind of workflow strategy we have now.

There are three events that will initiate a workflow run:

## Any PR and monitor the main branch

Implemented in [`check.yaml`](check.yaml)

A comprehensive but quick test workflow.
Triggered if the PR is created or the main branch is updated.

## Release publication

Implemented in [`release.yaml`](release.yaml)

Start the release process. It will do all possible checks and publish to crates.io.
Triggered if tag with 'v' is created.

## Nightly

Implemented in [`nightly.yaml`](nightly.yaml)

Run the full tests without considering how long it will take.
Triggered everyday.

## uProtocol specification compatibility

The uProtocol specification is evolving over time. In order to discover any discrepancies between the currently implemented version and the changes being introduced to the specification, we perform a nightly check that verifies if the current up-rust code base on the main branch can be compiled and test can be run successfully using the most recent revision of the uProtocol specification.

This is implemented in [`latest-up-spec-compatibility.yaml`](latest-up-spec-compatibility.yaml)

## Further workflow modules

In addition to the main workflows described above, there exist a number of modules that are used by these main workflows. These live in the [uProtocol CI/CD repository](https://github.com/eclipse-uprotocol/ci-cd)
