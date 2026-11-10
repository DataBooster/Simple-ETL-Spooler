# Simple ETL Spooler

<p align="right"><a href="README_cn.md">[简体中文]</a></p>

Beyond “powerful” lies another kind of vitality: simplicity.

Instead of asking each component to do more and more, keep it simple and reliable, then combine the parts to serve a larger purpose. Simple ETL Spooler is such a layer, simple enough to be transparent. It strips away business semantics to focus on declared data movement.

In Simple ETL Spooler, a Task holds EXTRACT in one hand and LOAD in the other, transparently adapting between them. Each side does what it does best, keeping local complexity low, while the Task contains the differences between them. A Task is preserved as a clear contract; multiple Tasks form a clear flow; the whole process is observable, diagnosable, and replayable.

## Quick Start

To start, think of it in two levels:

- A Task declares a minimal unit of data movement and carries its full context.
- A Batch groups related Tasks and shows how they are orchestrated over time.

A typical use follows just three steps:

| `CREATE_BATCH` |  | `ADD_TASK` |  | `START_BATCH` |
|:---:|:---:|:---:|:---:|:---:|
| Create a batch | ➔ | Add a task | ➔ | Start execution |

Here is an example:
```sql
DECLARE
    v_batch_id  SIMPLE_INTEGER;
BEGIN
    v_batch_id := {Schema}.UTIL_SYS_ETL.CREATE_BATCH(
        inComment        => 'Demo: sync data from source to target',
        inScheduled_Time => SYSDATE
    );
    {Schema}.UTIL_SYS_ETL.ADD_TASK(
        inBatch_ID         => v_batch_id,
        inSerially         => TRUE,
        inExtract_Type     => 'SQL',
        inExtract_Source   => 'SDBXYZ',
        inExtract_Command  => 'select PORT_CODE, MARKET_VALUE from SOME_SOURCE where AS_OF_DATE = :AS_OF_DATE',
        inExtract_Params   => '{"AS_OF_DATE":"2026-07-31"}',
        inLoad_Type        => 'SP',
        inField_Mapping    => '{"PORT_CODE":"in_PORT_CODE","MARKET_VALUE":"in_MARKET_VALUE"}',
        inMerge_Params     => '{}',
        inLoad_Destination => 'EDBXYZ',
        inLoad_Command     => 'SOME_TARGET_PACKAGE.LOAD_DATA',
        inTask_Comment     => 'Load latest market value snapshot'
    );
    {Schema}.UTIL_SYS_ETL.START_BATCH(v_batch_id);
    DBMS_OUTPUT.PUT_LINE(
        {Schema}.UTIL_SYS_ETL.WAIT_BATCH(
            inBatch_ID => v_batch_id,
            inTimeout_Minutes => 30
        )
    );
END;
/
```

> Note: `{Schema}` is the actual schema name used in your deployment.

This example has one Task; call `ADD_TASK` for each Task. `ADD_TASK` details come later. In practice, a lightweight scenario-specific wrapper can simplify it further.

To wait synchronously for a Batch to finish:

- `{Schema}.UTIL_SYS_ETL.WAIT_BATCH`

To replay a previously run Batch:

- `{Schema}.UTIL_SYS_ETL.RERUN_BATCH`

For now, the picture is simple:

A Task connects EXTRACT and LOAD, transparently adapting between them; `STEP_PLAN` lays out multiple Tasks as a clear flow within a Batch.

## Why It's Built This Way

### A Simple View of Systems

Simple ETL Spooler has no ambition to do everything. Serving the overall business purpose, it deliberately takes on less, leaving business logic where it belongs. The idea is simple: clear boundaries, simple actions, rich composition. Simplicity confines complexity that is hard to eliminate within its own space, allowing each part to do what it does best.

In this view:

- EXTRACT stays where EXTRACT works best.
- LOAD stays where LOAD works best.
- Simple ETL Spooler takes care of contract persistence, transparent adaptation, and Batch orchestration at runtime.
- Higher business layers decide why something should happen and which Tasks should happen.

Each part works in its own way, leaving local complexity low. Clear boundaries stop that complexity from spilling into the rest of the system, keeping the whole simpler.

In the wild, a stronger individual may have a better chance of survival. Software is different: internal competition wastes effort; what matters is how well the parts work together. Good collaboration depends less on how much each part can do than on how far complexity travels from its source before being absorbed. A local shortfall usually stays local and can be made up elsewhere. Complexity that escapes its source, however, can quietly spread through the system, eroding the vitality of the whole.

### Task: Connect or Take Over?

Every Task first triggers EXTRACT and, on success, triggers LOAD. That does not put the Task itself in a third execution role. It simply connects the two and adapts data between their contracts, leaving each side to do what it does best.

A Task follows four simple steps:

1. Trigger EXTRACT at the source
2. Receive the EXTRACT result
3. Adapt the result to the LOAD contract
4. Trigger LOAD at the destination

By combining Action Types with Source/Destination pairs, the same Task model can connect heterogeneous systems for data movement. The current matrix is:

| Data Movement | Action Type | Source / Destination |
|:---|:---|:---|
| 📤 **Extract** | SQL, SP | Oracle, SQL Server, Snowflake |
|  | MDX | SSAS Cube, Adaptiv MemoryCube |
|  | REST | RESTful API URL |
| 📥 **Load** | SP | Oracle, SQL Server, Snowflake |

The matrix also shows a clear asymmetry: EXTRACT supports SQL, SP, MDX, and REST, while LOAD currently uses SP only.

The same fields carry different meanings for different Action Types:

| Action Type | Source / Destination | Command | Params |
|:---|:---|:---|:---|
| SQL / SP | Database instance or connection configuration | SQL statement or stored procedure | Variables or parameters as a JSON object |
| MDX | Connection string | MDX query | Not used |
| REST | RESTful API URL | HTTP body / payload | HTTP headers, method, and other request metadata as a JSON object |

> **Note:** Snowflake SP calls require `CALL` syntax, such as `CALL schema_a.sp_b(:param1, ...)`, instead of the procedure name alone used by Oracle and SQL Server. At runtime, Simple ETL Spooler translates named variables (for SQL statements) or named parameters (for SP calls), both prefixed with a single `:`, to the positional-binding syntax required by the Snowflake driver.

With the same small set of fields, a Task can describe different Action Types while a common contract abstracts away implementation differences across source and destination. The same abstraction also extends to the `ADD_TASK` interface exposed to callers. The optional JSON objects `inExtract_Params`, `inField_Mapping`, and `inMerge_Params` let callers declare source-side inputs, field mappings, and additional destination-side inputs without handling the underlying adaptation themselves.

### The How, Not the What

Since Simple ETL Spooler's role is to provide transparent adaptation and coordinate execution at runtime, it is concerned not with what happens in the business, but with how those already-defined actions happen.

At this layer, Simple ETL Spooler is better suited to answering questions such as these: How should a data-movement contract be persisted in an executable form? How should an action be handed off to the endpoint best suited to perform it? And how should multiple actions be explicitly organized, scheduled, observed, and replayed?

From this perspective, Simple ETL Spooler is more like a diligent mail carrier. It does not care about the business content of a letter or why it needs to be sent at that particular moment. What matters here is a lower-level, more general set of questions: where to pick up the letter, where to deliver it, how to package and hand it off along the way, how to organize multiple letters into batches and routes, and how to keep the entire delivery traceable and replayable.

What and how are relative rather than absolute. Across system layers, the what of one layer often becomes part of the how of the layer above.

Principles such as single responsibility, separation of concerns, layering, and decoupling are as familiar to software practitioners as the air around them. Precisely because they are so familiar, they can easily be reduced to labels, terminology, or even formal doctrine. As delivery pressure accumulates, systems grow, and new requirements keep arriving, people naturally tend to structure systems in ways that can show noticeable progress as early as possible. The problem is that the direction imposed by that pressure does not always align with the system's most natural boundaries. Quite often, boundaries drawn directly from end requirements are orthogonal to those that follow the underlying nature.

Imagine a restaurant kitchen organized around individual dishes. Each chef takes one dish from beginning to end, leaving the stove whenever needed: out for ingredients, back to prepare and cook them, then finally to the table. In the streets, chefs crisscross like mail carriers, each hurrying back to their own dish. The arrangement would probably strike us as rather odd.

Yet from the perspective of a single dish, this arrangement makes perfect sense. One chef takes care of the dish from beginning to end. Other dishes belong to other chefs. Each dish stands on its own, while the whole table brings them together at another level. Single responsibility, separation of concerns, layering, decoupling. It all sounds entirely proper.

The problem, then, may lie not in the principles themselves, but in the way the boundaries are drawn. In software, the cost is rarely as visible as it is in a restaurant kitchen. Data movement, orchestration, and runtime responsibilities that naturally cut across business flows can readily be bundled back into units organized around end requirements. Such arrangements look nowhere near as absurd and often continue to appear reasonable and work just fine. Their cross-cutting nature points toward a boundary along another dimension.

### Why Isolate Complexity?

Three kinds of concerns tend to become entangled with the business flow:

- **Data movement:** EXTRACT and LOAD operations creep into higher-level business logic, gradually making the business intent harder to see.
- **Orchestration:** dependencies and execution order disappear into application code, making the process harder to follow, change, and trace.
- **Execution context:** when something fails, reconstructing what happened means piecing together scattered traces, making diagnosis cumbersome and precise replay difficult.

Picture a glass jar of cloudy water left undisturbed. Fine sediment suspended throughout the water slowly begins to settle. Likewise, data-movement details, orchestration relationships, and execution context that would otherwise remain dispersed through the business flow can be drawn together and made explicit. The water begins to clear.

Each Task becomes a transparent capsule: a complete data-movement contract preserved beyond the moment of execution. A Batch holds related capsules together on a logical tray; `STEP_PLAN` lays out their ordering and dependencies across that tray.

## How It Works

### The Runtime Essentials

#### Task: A Data Movement Contract

A Task is a transparent capsule holding a complete data-movement contract that persists beyond the moment of execution. It brings together everything needed for that movement to take place: how to reach the source and destination, how source results are adapted to destination inputs, and what context must be carried at runtime.
Inside that capsule, most matching between source results and destination inputs is implicit. `FIELD_MAPPING` makes only the differences explicit.

| Source Result |  | Adaptation |  | Destination Parameters |
|:---|:---:|:---|:---:|:---|
| **Array of row objects**<br>`{ column: value, ... }[]` | **➔** | `FIELD_MAPPING` | **➔** | **Named parameter arrays**<br>`{ param: value[], ... }` |
|  |  | `MERGE_PARAMS` | **➔** | **Additional scalar parameters**<br>`{ param: value, ... }` |
|  |  |  | **↓↓↓** | **Special key `"*"`**<br>`{ "*": "json_packet" }` |
|  |  |  |  | **`json_packet`** `=>`<br>`{`<br>&nbsp;&nbsp;`array_param: value[],`<br>&nbsp;&nbsp;`...,`<br>&nbsp;&nbsp;`scalar_param: value,`<br>&nbsp;&nbsp;`...`<br>`}` |

In practice, the following patterns apply:

- **Same names:** Source columns bind directly to destination parameters; no explicit mapping is needed.
- **Different names:** Only differences need to be declared. For example, `{"Market Value":"in_MARKET_VALUE"}` maps one name to another. A source column can also be explicitly discarded by mapping it to `null` or `""`.
- **Extra source columns:** Columns not needed by the destination are ignored automatically, just as if they were mapped to `null` or `""`.
- **Missing source columns:** Destination parameters not supplied by the source are omitted from the call, allowing defaults defined in the destination to take effect.

The source result starts as an array of row objects, each holding column-value pairs. The adaptation transposes the matched column values into named parameter arrays, then merges in the additional scalar parameters from `MERGE_PARAMS`, overriding any parameters with the same name.

- **Highly heterogeneous structures:** With the special `*` mapping, the resulting parameters are further packaged as JSON and passed to the destination through a single parameter, for example `{"*":"json_packet"}`. This is especially useful when the destination is a generic receiver without a fixed parameter signature. It also works well when the source already returns JSON rather than tabular data, as with REST, where conventional column mapping often does not apply. In such cases, passing the JSON through as-is is natural and broadly applicable.

Neither the source output nor the destination interface needs to be reshaped to fit the other. The transparent capsule holds the adaptation in the middle, leaving both sides as close as possible to their native forms.

#### Batch: Tasks as a Whole

A Batch is the logical tray where related Tasks are arranged as a whole. It does not perform the data movement itself. Once on the same tray, the Tasks can be controlled together, while their progress and overall status are tracked at the Batch level. The caller can therefore see where the Batch stands, wait for it to finish, or replay it.

That tray also has a place in time. Through `inScheduled_Time`, the caller can specify when the Batch’s Task flow should begin.

New Batches can emerge dynamically as a Batch’s Task flow progresses. A source-side or destination-side stored procedure triggered by a Task within that Batch can create new Batches in response to runtime data. For example, Batches may cascade from one to another or branch from a parent Batch as needed. When creating a new Batch, the optional `inParent_Batch_ID` parameter records its parent, keeping the lineage of Batches traceable. Dynamic growth allows the broader flow to adapt and extend itself at runtime.

#### STEP_PLAN: Task Flow at a Glance

`STEP_PLAN` is more than just a sequence number; it indicates a Task’s stage in a Batch’s overall flow. Tasks sharing the same `STEP_PLAN` belong to the same parallel stage. Stages proceed in value order, with each stage starting only after all Tasks in the previous stage have completed. The resulting flow looks like this:

| STEP_PLAN 1 |  | STEP_PLAN 2 |  | STEP_PLAN 3 |
|:---|:---:|:---|:---:|:---|
| Task A<br>Task B<br>Task C | ➔ | Task D<br>Task E | ➔ | Task F<br>Task G |

When something goes wrong, this view shows where the flow stopped and whether the failure occurred on the source or destination side of a specific Task. From there, the underlying cause can be traced and fixed closer to its origin; replay is Batch-level.

### Orchestration: Code or Data?

Systems often grow heavy not only as more actions are added, but as the relationships among them become buried in code. How Tasks are grouped and how their flow is coordinated can become entangled with lower-level implementation details and higher-level business logic. Orchestration then ends up scattered across places such as application code, scheduler configurations, and deeply nested workflows.

Simple ETL Spooler takes a different approach: it expresses those relationships as data. Each Task carries an individual data-movement contract. A Batch brings related Tasks together, while each Task’s `STEP_PLAN` indicates its stage within the flow of that Batch. The flow is laid out directly rather than reconstructed from implementation logic, making it easier to see and adjust.

This is not a choice between data and code. Code remains responsible for the actual EXTRACT and LOAD actions. Their linkage within each Task and the further organization of Tasks into a flow are expressed separately as concise data. The clearer this boundary is, the more coherent each side can remain internally, and the easier the system is to maintain as a whole.

### Persist, Then Run

In many implementations, business flows are woven into code alongside the machinery that carries them out, sharing the same development lifecycle. Code is designed to do its work quickly and move on; each run’s trail and critical context therefore remain only briefly, fading as the calls pass. This transience works naturally for the machinery; business flows can live with it and still get the work done. But if a run fails along the way, pinpointing where the failure occurred and reconstructing what happened there can take considerable effort.

Simple ETL Spooler reverses the order: first persist the related contracts and necessary context, then start the flow. The run unfolds from what is already on record, rather than leaving the full picture to be pieced together afterward.

Persisting the whole Batch first lets the run proceed with its context already on record. The runtime machinery uses that persisted context directly, while the same context remains available for other needs such as status aggregation, performance analysis, auditing, troubleshooting, and replay.

Seen this way, Simple ETL Spooler is not a conveyor belt that wipes away its own footprints as it moves. It is closer to a line where each package is registered before departure, then sent along its route. The flow can then move forward while the full picture remains available to look back on when needed.

### See the System as It Runs

With that picture in place, the system can see more than whether a run succeeded or failed: where a Batch stands, which stage it has reached, which Tasks have finished and which have yet to advance, and where a failure surfaced along the flow. Runtime state is overlaid on the persisted picture to form a live view of the flow.

| STEP_PLAN 1 |  | STEP_PLAN 2 |  | STEP_PLAN 3 |
|:---|:---:|:---|:---:|:---|
| ✓ Task A<br>✓ Task B<br>✓ Task C | ➔ | ● Task D<br>✓ Task E | ➔ | ○ Task F<br>○ Task G |

The runtime states behind that view are queryable and traceable. Common states include `DRAFTING`, `READY_TO_RUN`, `RUNNING`, `COMPLETED`, and `COMPLETED_WITH_ERROR`.

So at any moment, the system can answer: How is the work going?

The runtime view feels more like a dashboard than a pile of logs. Troubleshooting no longer has to rely on whatever can be pieced together from logs written separately along the way. A failure’s exact location in the flow no longer needs to be inferred indirectly. Batch, stage, status, and context are already brought together in the same runtime view.

That runtime view is exposed most directly through two database views:

- `{Schema}.VIEW_UTIL_SYS_ETL_BATCH_STATUS`: one row per Batch
- `{Schema}.VIEW_UTIL_SYS_ETL_TASK_STATUS`: one row per Task

To keep the runtime engine lightweight and responsive, completed Batch records are automatically moved to archive tables. The two views show active and archived records together, hiding the underlying storage split. The caller gets a consistent view across the full lifecycle without having to manually combine records from both sides of that split.

Common queries look like this:

```sql
SELECT * FROM {Schema}.VIEW_UTIL_SYS_ETL_BATCH_STATUS ORDER BY BATCH_ID DESC;
SELECT * FROM {Schema}.VIEW_UTIL_SYS_ETL_TASK_STATUS ORDER BY BATCH_ID DESC, STEP_PLAN, TASK_ID;
```

In a failure scenario, these two views provide a direct drill-down path: the Batch view identifies the run, and the Task view pinpoints the failing Task. Within that Task, `COMPLETED_TIME` shows whether it has finished, while `RUNTIME_ERROR` carries the error details, if any. An “Extracting Error:” or “Loading Error:” prefix then indicates the failing side. The text that follows is the error message returned verbatim from that side. If richer diagnostic detail is needed, it is best added at the source or destination itself. After the underlying issue is fixed, `RERUN_BATCH` can be used to rerun the Batch from its persisted contract.

Batch start and end times, together with Task completion times, naturally form a performance baseline across runs. That baseline provides a practical reference when tuning the data-movement path and parallel arrangement, or when investigating a slowdown on the source or destination side of a Task. Exact per-Task start times are not currently recorded, avoiding extra database writes just for finer timing. A future implementation could write that timing back with Task completion.

Observability here goes beyond looking back at what happened. The live view exposes the same context the runtime machinery uses to advance the flow, with nothing separately prepared for observability. So there is less redundant information to sift through and less risk of missing what matters. The overall state of the flow remains visible while the work is still unfolding.

## Making It Work in Your Enterprise
So what is running behind all this, polling the queue and keeping the flow moving? In the current open-source implementation, it is a C# Windows Service.

The name *Spooler* draws on the same idea as a Print Spooler, but applies it to data movement. It picks up each ready Batch and dispatches its Tasks along the planned flow. It also coordinates linkages and interactions among the endpoints involved.

The open-source code is structured for enterprise adoption, keeping potential enterprise-specific modifications isolated and confined to a few well-defined areas. This makes the Spooler easier to customize without touching most of the system.

The intended integration points are clearly marked with `customize...` comments in the code and database installation scripts. They isolate the code-level adaptations for integrating the Spooler with your enterprise services and data sources. Other environment-specific differences and the Spooler’s own runtime parameters are handled through configuration.

Deployment itself is straightforward: use the installation scripts to set up the control database, install the Windows Service on a Windows server, and verify its connection to that database.

Multiple Windows Service nodes can share the same control database, but each Batch is still picked up as a whole by one node.

Once deployed, the Spooler largely recedes into the background. Data-movement development and support remain focused on the SQL interfaces and runtime views rather than the machinery behind them.

This implementation supports endpoint types including Oracle, SQL Server, Snowflake, MDX, and REST. Additional types can be added just by extending the adaptation layer. REST and Snowflake, for example, were added over time. On the control side, the Spooler queue and application-facing APIs such as `CREATE_BATCH`, `ADD_TASK`, `START_BATCH`, `WAIT_BATCH`, and `RERUN_BATCH` are currently implemented on Oracle. Migrating the control database to another database platform would require corresponding changes to the database scripts and Windows Service code.

Customization, endpoint support, the choice of control database, and even the runtime mechanism are still matters of implementation. More fundamental, though, is the underlying design thinking: clear boundaries, contained complexity, transparent adaptation, and observability.

## Where It Fits

The Spooler is not meant to become a place where every concern eventually collects. Its scope is much narrower:

- the required data movement can be declared explicitly and concisely;
- the same recurring runtime concerns would otherwise be recreated within applications or woven into business flows, duplicating effort and embedding complexity throughout the system.

That makes the Spooler particularly useful when:

- similar data-movement patterns recur across endpoint pairs, especially heterogeneous ones;
- related Tasks lend themselves to some sequential and parallel arrangements;
- runtime context remains valuable throughout and beyond a run;
- applications want to stay clear of low-level EXTRACT / LOAD mechanics and remain focused on their business intent.

The name *Spooler* itself hints at where it fits. Its natural setting is batch-oriented data movement, much like scheduled transit. Work is known before it sets out. It is arranged into a planned flow. It is carried forward through a shared queue mechanism geared toward overall throughput. That means work follows a shared rhythm, rather than every piece getting its own immediate ride.

Some work follows a very different rhythm. Take very low-latency transactions, for example. This kind of work is more like an on-demand ride, where immediate response and low latency take priority. Event-driven or callback-based mechanisms may then be a more natural fit.

Every mechanism has a boundary of its own. The nature of the problems can be very different, so they do not need to be addressed in the same way. Those boundaries are more about what fits where than about what is more advanced. The Spooler, for its part, should not quietly grow beyond its boundaries and into an all-purpose hub for business scheduling, rule processing, and heavyweight data integration.

The deeper question is not which mechanism should win. More fundamentally, what kind of systematic thinking shapes those choices? How do we recognize their natural boundaries and make the best use of each where it fits, so the system as a whole stays simple?

## Simplicity That Can See Itself

Simplicity alone does not give a system vitality. Complexity, by contrast, can quietly erode that vitality; once it gets out of control, the damage can become fatal. The real challenge is not to choose between simplicity and complexity, but to keep achieving the right goals without letting the work become complicated in the wrong ways.

Drift toward complexity and expansion comes easily. Restraint does not. One locally reasonable decision can follow another until boundaries blur, responsibilities widen, and concerns once separated find their way back together. Nothing has to look obviously wrong. That is why simplicity cannot remain a slogan. It requires the discipline to keep seeing what the system is becoming as it evolves. It also requires looking past labels to the substance behind them. Once that awareness fades, complexity can expand long before the damage becomes visible.

Reflection also needs a clear chain of external evidence. That matters especially amid the pull of technological fashion and a profusion of architectural prescriptions. In the Spooler, end-to-end visibility, performance history, and retained runtime context form a dashboard for the movement pipeline. The readings keep us anchored in what the system is actually doing and help puncture deep-seated notions that might otherwise go unquestioned.

A car can still roar onto the road without a dashboard. A person without the habit of self-reflection can still work and live much like anyone else. With little self-questioning, a person may even speak more confidently, more forcefully, and with greater authority. Yet a black box can keep running for a long time while consuming itself from within.

That is precisely the path Simple ETL Spooler tries to avoid. Simple persistence, visible dependencies, and a clear record of the full run are not merely conveniences; together they preserve a practical capacity for self-observation: Observability. Each action remains distinct; each failure leaves a clear scene and a short path to repair.

Simplicity that can continue to see itself is an important source of durable vitality.
