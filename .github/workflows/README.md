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

## Other workflow modules

Besides the 3 workflows above, there are other workflows used by them:

- [`coverage.yaml`](coverage.yaml): Collect the code coverage information.
- [`verify-msrv.yaml`](verify-msrv.yaml): Checks if the MSRV (Minimum Supported Rust Version) declared in Cargo.toml is correct
- [`x-build.yaml`](x-build.yaml): Run release builds on multiple architecture targets
