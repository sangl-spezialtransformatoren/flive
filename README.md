# flive
## Flow Lifecycle

```mermaid
stateDiagram-v2
    classDef associated fill:#FFA500
    WAITING: Waiting
    SCHEDULED: Scheduled
    state ORCHESTRATOR_LOST <<choice>>
    RUNNING: Running
    ORPHANED: Orphaned
    COMPLETED: Completed
    FAILED_FINALLY: Failed Finally
    state FAILED <<choice>>

    [*] --> WAITING: dispatch
    WAITING --> SCHEDULED: acquire
    [*] --> SCHEDULED: dispatch and acquire
    SCHEDULED --> ORCHESTRATOR_LOST: orchestrator lost
    SCHEDULED --> RUNNING: start work
    ORCHESTRATOR_LOST --> WAITING: has no parent
    RUNNING --> ORCHESTRATOR_LOST: orchestrator lost
    RUNNING --> COMPLETED: complete
    RUNNING --> FAILED: fail
    FAILED --> SCHEDULED: retries left
    FAILED --> FAILED_FINALLY: no retries left
    ORCHESTRATOR_LOST --> ORPHANED: has a parent
    COMPLETED --> [*]
    FAILED_FINALLY --> [*]
    ORPHANED --> SCHEDULED: recover

    class SCHEDULED,RUNNING,FAILED associated
```

Note: Each arrow in the diagram represents a database transaction. The diamond-shaped choice nodes (ORCHESTRATOR_LOST and FAILED) occur within their respective transactions, ensuring atomicity of state transitions.

Note: The orange-colored states (Scheduled, Running, Failed) indicate that the flow is associated with an orchestrator during these stages. This association ensures proper management and tracking of the flow's lifecycle by the assigned orchestrator.


