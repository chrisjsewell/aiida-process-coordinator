# aiida-task-controller

A proof of principle for a non-RabbitMQ `Process` task control solution.

## Why?

In short, since <https://github.com/rabbitmq/rabbitmq-server/pull/2990>, RabbitMQ >= 3.7 will timeout a task if it is not finished within 30 minutes; much shorter than most use cases in AiiDA.
As discussed in <https://github.com/rabbitmq/rabbitmq-server/discussions/3345>, this timeout is not controllable on the client side (i.e. by AiiDA), and will mean all users will have to manually add configuration on the server side, to turn off this timeout.
This will make AiiDA significantly less user friendly and, even if documented, will likely result in a lot of confusion and issues raised.

In detail ...

## Requirements for the controller

These are the major requirements for the controller:

- Ensure every submitted `Process` is run to termination
- Ensure each `Process` is only being executed by one daemon worker at any time
- Handle nested submissions, i.e. processes that submit sub-processes, and must wait on their completion before continuing
- Handle pause/play and kill requests

## Proposed solution

...

## TODO

- Use circus plugins like: https://circus.readthedocs.io/en/latest/for-ops/using-plugins/#flapping
- Also think about where Process checkpoint lives
