# TaskForge

TaskForge is a distributed background job scheduling and execution system built with Go.

It provides a central server for managing tasks and workers that execute jobs asynchronously.

## Features

- Background task scheduling
- Distributed worker execution
- Task status tracking
- Persistent task storage
- HTTP API
- Web dashboard
- Prometheus metrics
- Docker support

## Tech Stack

- Go
- SQLite
- Docker
- Prometheus
- REST API

## Running Locally

Install Go and clone the repository.

Then run:

```bash
go run ./cmd/server