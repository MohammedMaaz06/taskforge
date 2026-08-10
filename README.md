# TaskForge

A distributed background job scheduling and execution engine built in Go.

## Architecture
- **Server:** Manages job queues and schedules tasks.
- **Worker:** Polls and executes queued tasks concurrently.
- **Store:** Keeps track of task statuses and execution logs.
