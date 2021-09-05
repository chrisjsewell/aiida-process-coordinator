# aiida-process-coordinator

A prototype for a non-RabbitMQ `Process` execution coordinator.

The solution builds on aiida-core's existing database and daemon management infrastructure,
with no additional dependencies.

## Terminology

- **Process**: An AiiDA `Process` instance, stored in the database
- **Worker**: A system process, that can execute one or more `Process` to termination
- **Daemon**: A system process, which manages spawning, monitoring and killing of child processes (e.g. workers) and sockets

## What are the requirements for the process coordinator?

These are the major requirements for the coordinator:

- Ensure every submitted AiiDA `Process` is run to termination
- Ensure each AiiDA `Process` is only being executed by one worker at any time
- Handle nested submissions, i.e. Processes that submit sub-Processes, and must wait on their completion before continuing
- Handle pause/play and kill requests for Processes

## Why replace RabbitMQ?

In short:

Since [rabbitmq-server#2990](https://github.com/rabbitmq/rabbitmq-server/pull/2990), RabbitMQ >= 3.7 will timeout a task if it is not finished within 30 minutes; much shorter than most use-cases in AiiDA.
As discussed in [rabbitmq-server#3345](https://github.com/rabbitmq/rabbitmq-server/discussions/3345), this timeout is not controllable on the client side (i.e. by AiiDA), and will mean all users will have to manually add configuration on the server side to turn off this timeout.
This will make AiiDA even less user friendly to get started with and, even if documented, will likely result in a lot of confusion and issues raised.

In detail:

Below is the current schematic of submitting a `Process` in AiiDA:

<img src="./images/rabbitmq-sysml.svg" style="height: 350px;" />

1. The user starts the AiiDA daemon which spawns one or more workers, that connect as clients (a.k.a consumers) to the RabbitMQ server
2. The user submits a `Process`
3. AiiDA stores the initial `Process` in the database
4. AiiDA sends a task to the RabbitMQ server task queue
5. The RabbitMQ server sends out these task messages to each worker in turn, for one to consume, waiting for an ack(nowledgment)/nack/reject response (see also [Consumer Acknowledgements](https://www.rabbitmq.com/confirms.html))
6. One worker will execute the task and then (ack)nowledge message receipt to the RabbitMQ server (and the task is removed from the queue)
7. If the executing worker loses connection with the RabbitMQ server, the task will be requeued and sent to another worker

This current design has a number of shortcomings:

- It requires users to install and configure RabbitMQ on the machine running AiiDA
  - As mentioned above, this will require additional, advanced configuration for RabbitMQ >= 3.7
  - This increases the learning/deployment barrier for new users
  - RabbitMQ is generally not well understood by new users, or even AiiDA maintainers, and so is difficult to debug/fix issues
  - RabbitMQ is a non-Python dependency, and so it is not as easy for AiiDA to pin version requirements (must use Conda)
- RabbitMQ must be running before any `Process` can be submitted, or actions taken (pause/play/kill)
- This essentially introduces two independent sources where the state of the `Process` is persisted: in the database and in the RabbitMQ server
  - This can lead to "de-syncing" of the sources; if the worker-RabbitMQ connection is lost (leading to RabbitMQ tasking a worker to execute an already running task [aiida-core#4598](https://github.com/aiidateam/aiida-core/issues/4598)), or if the task is lost (leading to `Process`s being left in an un-terminated state [aiida-core#1712](https://github.com/aiidateam/aiida-core/issues/1712))
- To my knowledge, there is no easy way for the user to introspect RabbitMQ; to find out what tasks are outstanding or running.
- It is not easy to backup/move an AiiDA profile with unfinished processes, as this would require also require moving the RabbitMQ's persistent data.

## Proposed solution

As shown below, the proposed solution essentially replaces the RabbitMQ server with a (singleton) `Process` execution coordinator, spawned by the AiiDA daemon, which reads from persisted data in the database.

<img src="./images/new-sysml.svg" style="height: 350px;" />

- The solution currently only uses existing AiiDA Python dependencies (sqlalchemy and circus)
- The persistence for executing processes is all situated in the database
  - Introspection/manipulation of this data can be easily implemented with standard database queries
  - The user can submit a process or set an action (pause/play/kill) on a process, with only a connection to the database
- The coordinator also handles "load balancing" of `Process` submitted to workers; submitting new processes to the worker with the least load (by contrast RabbitMQ will submit to the same worker, until it is full)

## The proposal by example

This package implements a mock AiiDA database with sqlite (created on demand), mock processes which wait for 60 seconds, and exposes a CLI.
It is recommended to run the CLI via [tox](https://tox.readthedocs.io/en/latest/), to auto-create an isolated Python environment.

First we submit a number of process to the database:

```console
$ tox database submit 10
$ tox database status
```

This will generate an `aiida.sqlite3` file in the current directory.
If we inspect the database (for example using [vscode-sqlite](https://marketplace.visualstudio.com/items?itemName=alexcvzz.vscode-sqlite)), the standard node table (`db_dbnode`) is populated:

<table><tr><th>id</th><th>mtime</th><th>status</th><tr><tr><td>1</td><td>2021-09-05 01:58:12.899992</td><td>created</td></tr><tr><td>2</td><td>2021-09-05 01:58:12.913475</td><td>created</td></tr><tr><td>3</td><td>2021-09-05 01:58:12.923336</td><td>created</td></tr><tr><td>4</td><td>2021-09-05 01:58:12.933164</td><td>created</td></tr><tr><td>5</td><td>2021-09-05 01:58:12.942143</td><td>created</td></tr><tr><td>6</td><td>2021-09-05 01:58:12.953719</td><td>created</td></tr><tr><td>7</td><td>2021-09-05 01:58:12.964585</td><td>created</td></tr><tr><td>8</td><td>2021-09-05 01:58:12.979336</td><td>created</td></tr><tr><td>9</td><td>2021-09-05 01:58:12.991934</td><td>created</td></tr><tr><td>10</td><td>2021-09-05 01:58:13.002071</td><td>created</td></tr></table>

Then, there is also a `db_dbprocess` table, with outstanding processes to execute:

<table><tr><th>id</th><th>mtime</th><th>dbnode_id</th><th>action</th><th>worker_pid</th><th>worker_uuid</th><tr><tr><td>1</td><td>2021-09-05 01:58:12.906869</td><td>1</td><td>NULL</td><td>NULL</td><td>NULL</td></tr><tr><td>2</td><td>2021-09-05 01:58:12.917708</td><td>2</td><td>NULL</td><td>NULL</td><td>NULL</td></tr><tr><td>3</td><td>2021-09-05 01:58:12.928120</td><td>3</td><td>NULL</td><td>NULL</td><td>NULL</td></tr><tr><td>4</td><td>2021-09-05 01:58:12.937583</td><td>4</td><td>NULL</td><td>NULL</td><td>NULL</td></tr><tr><td>5</td><td>2021-09-05 01:58:12.946685</td><td>5</td><td>NULL</td><td>NULL</td><td>NULL</td></tr><tr><td>6</td><td>2021-09-05 01:58:12.958601</td><td>6</td><td>NULL</td><td>NULL</td><td>NULL</td></tr><tr><td>7</td><td>2021-09-05 01:58:12.972674</td><td>7</td><td>NULL</td><td>NULL</td><td>NULL</td></tr><tr><td>8</td><td>2021-09-05 01:58:12.985928</td><td>8</td><td>NULL</td><td>NULL</td><td>NULL</td></tr><tr><td>9</td><td>2021-09-05 01:58:12.996245</td><td>9</td><td>NULL</td><td>NULL</td><td>NULL</td></tr><tr><td>10</td><td>2021-09-05 01:58:13.010583</td><td>10</td><td>NULL</td><td>NULL</td><td>NULL</td></tr></table>

If we start the daemon, with two workers, the coordinator will distribute the processes evenly to the workers:

```console
$ tox daemon start 2
```

<table><tr><th>id</th><th>mtime</th><th>dbnode_id</th><th>action</th><th>worker_pid</th><th>worker_uuid</th><tr><tr><td>1</td><td>2021-09-05 02:06:20.220162</td><td>1</td><td>NULL</td><td>25267</td><td>345d6810-0a7c-465e-b513-c8ca16c64966</td></tr><tr><td>2</td><td>2021-09-05 02:06:20.226971</td><td>2</td><td>NULL</td><td>25266</td><td>4ae05820-8f88-4e8f-8ebd-68fa6182f162</td></tr><tr><td>3</td><td>2021-09-05 02:06:20.232902</td><td>3</td><td>NULL</td><td>25267</td><td>345d6810-0a7c-465e-b513-c8ca16c64966</td></tr><tr><td>4</td><td>2021-09-05 02:06:20.237730</td><td>4</td><td>NULL</td><td>25266</td><td>4ae05820-8f88-4e8f-8ebd-68fa6182f162</td></tr><tr><td>5</td><td>2021-09-05 02:06:20.243235</td><td>5</td><td>NULL</td><td>25267</td><td>345d6810-0a7c-465e-b513-c8ca16c64966</td></tr><tr><td>6</td><td>2021-09-05 02:06:20.249431</td><td>6</td><td>NULL</td><td>25266</td><td>4ae05820-8f88-4e8f-8ebd-68fa6182f162</td></tr><tr><td>7</td><td>2021-09-05 02:06:20.255207</td><td>7</td><td>NULL</td><td>25267</td><td>345d6810-0a7c-465e-b513-c8ca16c64966</td></tr><tr><td>8</td><td>2021-09-05 02:06:20.260356</td><td>8</td><td>NULL</td><td>25266</td><td>4ae05820-8f88-4e8f-8ebd-68fa6182f162</td></tr><tr><td>9</td><td>2021-09-05 02:06:20.265834</td><td>9</td><td>NULL</td><td>25267</td><td>345d6810-0a7c-465e-b513-c8ca16c64966</td></tr><tr><td>10</td><td>2021-09-05 02:06:20.270775</td><td>10</td><td>NULL</td><td>25266</td><td>4ae05820-8f88-4e8f-8ebd-68fa6182f162</td></tr></table>

If we stop the daemon part way through, and re-start with a single worker, the coordinator will re-assign the processes to that worker:

```console
$ tox daemon stop
$ tox daemon start 1
```

<table><tr><th>id</th><th>mtime</th><th>dbnode_id</th><th>action</th><th>worker_pid</th><th>worker_uuid</th><tr><tr><td>2</td><td>2021-09-05 02:12:06.070548</td><td>2</td><td>NULL</td><td>25754</td><td>4297ea48-b9a4-4f50-a00e-b983410104d0</td></tr><tr><td>4</td><td>2021-09-05 02:12:06.076692</td><td>4</td><td>NULL</td><td>25754</td><td>4297ea48-b9a4-4f50-a00e-b983410104d0</td></tr><tr><td>6</td><td>2021-09-05 02:12:06.081961</td><td>6</td><td>NULL</td><td>25754</td><td>4297ea48-b9a4-4f50-a00e-b983410104d0</td></tr><tr><td>8</td><td>2021-09-05 02:12:06.086706</td><td>8</td><td>NULL</td><td>25754</td><td>4297ea48-b9a4-4f50-a00e-b983410104d0</td></tr><tr><td>10</td><td>2021-09-05 02:12:06.091502</td><td>10</td><td>NULL</td><td>25754</td><td>4297ea48-b9a4-4f50-a00e-b983410104d0</td></tr></table>

Once completed, the `db_dbprocess` table will be empty, and the `db_dbnode` table will contain all terminated processes:

<table><tr><th>id</th><th>mtime</th><th>status</th><tr><tr><td>1</td><td>2021-09-05 02:07:22.528752</td><td>finished</td></tr><tr><td>2</td><td>2021-09-05 02:13:08.312547</td><td>finished</td></tr><tr><td>3</td><td>2021-09-05 02:07:22.535125</td><td>finished</td></tr><tr><td>4</td><td>2021-09-05 02:13:08.316970</td><td>finished</td></tr><tr><td>5</td><td>2021-09-05 02:07:22.538566</td><td>finished</td></tr><tr><td>6</td><td>2021-09-05 02:13:08.320571</td><td>finished</td></tr><tr><td>7</td><td>2021-09-05 02:07:22.547629</td><td>finished</td></tr><tr><td>8</td><td>2021-09-05 02:13:08.327909</td><td>finished</td></tr><tr><td>9</td><td>2021-09-05 02:07:22.550764</td><td>finished</td></tr><tr><td>10</td><td>2021-09-05 02:13:08.330605</td><td>finished</td></tr></table>

If we look in the daemon log files, we can see how progression of the coordinator and workers:

`workdir/watcher-aiida-coordinator.log`

```log
INFO:server-501-25265:[STARTING] server is starting...
INFO:server-501-25265:[LISTENING] Server is listening on ('192.168.0.154', 59483)
INFO:server-501-25265:[NEW CONNECTION] ('192.168.0.154', 59486) connected.
INFO:server-501-25265:[ACTIVE CONNECTIONS] 1
INFO:server-501-25265:[('192.168.0.154', 59486)] process PID 25267
INFO:server-501-25265:[NEW CONNECTION] ('192.168.0.154', 59487) connected.
INFO:server-501-25265:[ACTIVE CONNECTIONS] 2
INFO:server-501-25265:[('192.168.0.154', 59487)] process PID 25266
INFO:server-501-25265:[DB] Submitting process 1 (node 1) to worker PID 25267
INFO:server-501-25265:[DB] Submitting process 2 (node 2) to worker PID 25266
INFO:server-501-25265:[DB] Submitting process 3 (node 3) to worker PID 25267
INFO:server-501-25265:[DB] Submitting process 4 (node 4) to worker PID 25266
INFO:server-501-25265:[DB] Submitting process 5 (node 5) to worker PID 25267
INFO:server-501-25265:[DB] Submitting process 6 (node 6) to worker PID 25266
INFO:server-501-25265:[DB] Submitting process 7 (node 7) to worker PID 25267
INFO:server-501-25265:[DB] Submitting process 8 (node 8) to worker PID 25266
INFO:server-501-25265:[DB] Submitting process 9 (node 9) to worker PID 25267
INFO:server-501-25265:[DB] Submitting process 10 (node 10) to worker PID 25266
INFO:server-501-25753:[STARTING] server is starting...
INFO:server-501-25753:[LISTENING] Server is listening on ('192.168.0.154', 59591)
INFO:server-501-25753:[NEW CONNECTION] ('192.168.0.154', 59594) connected.
INFO:server-501-25753:[ACTIVE CONNECTIONS] 1
INFO:server-501-25753:[('192.168.0.154', 59594)] process PID 25754
INFO:server-501-25753:[DB] Submitting process 2 (node 2) to worker PID 25754
INFO:server-501-25753:[DB] Submitting process 4 (node 4) to worker PID 25754
INFO:server-501-25753:[DB] Submitting process 6 (node 6) to worker PID 25754
INFO:server-501-25753:[DB] Submitting process 8 (node 8) to worker PID 25754
INFO:server-501-25753:[DB] Submitting process 10 (node 10) to worker PID 25754
```

`workdir/watcher-aiida-workers.log`

```log
INFO:worker-501-25267:[STARTING] worker is starting...
INFO:worker-501-25267:[CONNECTED] to server: ('192.168.0.154', 59483)
INFO:worker-501-25266:[STARTING] worker is starting...
INFO:worker-501-25266:[CONNECTED] to server: ('192.168.0.154', 59483)
INFO:worker-501-25267:[SUBMIT] process 1
INFO:worker-501-25266:[SUBMIT] process 2
INFO:worker-501-25267:[SUBMIT] process 3
INFO:worker-501-25266:[SUBMIT] process 4
INFO:worker-501-25267:[SUBMIT] process 5
INFO:worker-501-25266:[SUBMIT] process 6
INFO:worker-501-25267:[SUBMIT] process 7
INFO:worker-501-25266:[SUBMIT] process 8
INFO:worker-501-25267:[SUBMIT] process 9
INFO:worker-501-25266:[SUBMIT] process 10
INFO:worker-501-25267:[FINISH] process 1
INFO:worker-501-25267:[FINISH] process 3
INFO:worker-501-25267:[FINISH] process 5
INFO:worker-501-25267:[FINISH] process 7
INFO:worker-501-25267:[FINISH] process 9
INFO:worker-501-25754:[STARTING] worker is starting...
INFO:worker-501-25754:[CONNECTED] to server: ('192.168.0.154', 59591)
INFO:worker-501-25754:[SUBMIT] process 2
INFO:worker-501-25754:[SUBMIT] process 4
INFO:worker-501-25754:[SUBMIT] process 6
INFO:worker-501-25754:[SUBMIT] process 8
INFO:worker-501-25754:[SUBMIT] process 10
INFO:worker-501-25754:[FINISH] process 2
INFO:worker-501-25754:[FINISH] process 4
INFO:worker-501-25754:[FINISH] process 6
INFO:worker-501-25754:[FINISH] process 8
INFO:worker-501-25754:[FINISH] process 10
```

## TODO

- Implement handling of actions
- Use IPC sockets (over TCP)
- Use circus plugins like: https://circus.readthedocs.io/en/latest/for-ops/using-plugins/#flapping
- Also think about where Process checkpoint lives
- bi-directional heartbeat (ensure workers immediately stop their running processes, if the connection to the coordinator is lost, to void duplication)
- higher-level APIs?
  - https://docs.python.org/3/library/socketserver.html#module-socketserver
  - https://docs.python.org/3/library/asyncio-stream.html
