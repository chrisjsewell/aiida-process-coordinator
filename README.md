# aiida-task-controller

A proof of principle for a non-RabbitMQ `Process` task control solution.

## What are the requirements for the task controller?

These are the major requirements for the controller:

- Ensure every submitted AiiDA `Process` is run to termination
- Ensure each AiiDA `Process` is only being executed by one worker process at any time
- Handle nested submissions, i.e. processes that submit sub-processes, and must wait on their completion before continuing
- Handle pause/play and kill requests for processes

## Why replace RabbitMQ?

In short:

Since <https://github.com/rabbitmq/rabbitmq-server/pull/2990>, RabbitMQ >= 3.7 will timeout a task if it is not finished within 30 minutes; much shorter than most use cases in AiiDA.
As discussed in <https://github.com/rabbitmq/rabbitmq-server/discussions/3345>, this timeout is not controllable on the client side (i.e. by AiiDA), and will mean all users will have to manually add configuration on the server side to turn off this timeout.
This will make AiiDA even less user friendly to get started with and, even if documented, will likely result in a lot of confusion and issues raised.

In detail:

...

## Proposed solution

...

## TODO

- Use IPC sockets (over TCP)
- Use circus plugins like: https://circus.readthedocs.io/en/latest/for-ops/using-plugins/#flapping
- Also think about where Process checkpoint lives
- bi-directional heartbeat
