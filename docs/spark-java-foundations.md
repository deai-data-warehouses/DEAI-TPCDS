# Spark & Java Foundations

A beginner-friendly guide to what Spark is, why it needs Java, how memory works
in our pipeline, and why the first run of each benchmark query is slower.

> **How to read this document:** every section starts with a short answer
> (**TL;DR**), followed by a more detailed explanation. If you only have five
> minutes, read the TL;DRs.

---

## Table of contents

1. [Spark](#1-spark)
   - [What is Spark?](#11-what-is-spark)
   - [How is Spark different from a "regular" SQL database?](#12-how-is-spark-different-from-a-regular-sql-database)
   - [What is Spark's main objective?](#13-what-is-sparks-main-objective)
   - [Why would someone choose Spark?](#14-why-would-someone-choose-spark)
2. [Java](#2-java)
   - [Why does Spark need Java?](#21-why-does-spark-need-java)
   - [What is the JVM and why is it necessary?](#22-what-is-the-jvm-and-why-is-it-necessary)
3. [Spark + Java memory: why we force the garbage collector](#3-spark--java-memory-why-we-force-the-garbage-collector)
4. [Why is the first run of every query slower?](#4-why-is-the-first-run-of-every-query-slower)
5. [Glossary](#5-glossary)
6. [References](#6-references)

---

## 1. Spark

### 1.1 What is Spark?

> **TL;DR:** Spark is an engine that processes large amounts of data by
> splitting the work into many small pieces and running them in parallel.

Imagine you need to count every word in 10,000 books. Doing it alone takes
forever. Instead, you hand one book to each of 100 friends, everyone counts
their part, and then you add up the results. **Spark is the organizer of that
process**, but for data.

You tell Spark *what* you want (with SQL, DataFrames, or Python/Scala/Java
code). Spark then:

1. Builds a **plan** for how to compute it.
2. Splits the data into **partitions** (chunks).
3. Runs one **task** per partition, in parallel.
4. Combines the partial results into the final answer.

Spark has two kinds of components:

```
                ┌──────────────────────┐
                │        DRIVER        │  ← "the boss": plans the work,
                │   (your SparkSession)│    hands out tasks, collects results
                └──────────┬───────────┘
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
   ┌────────────┐   ┌────────────┐   ┌────────────┐
   │  EXECUTOR  │   │  EXECUTOR  │   │  EXECUTOR  │  ← "the workers": run
   │  (tasks)   │   │  (tasks)   │   │  (tasks)   │    tasks on data chunks
   └────────────┘   └────────────┘   └────────────┘
```

**In our project:** we use `.master("local[4]")` (see
[`pipeline/constants.py`](../pipeline/constants.py)). This means the driver
*and* the executors all live inside **a single process on one machine**, using
**4 worker threads**. It is the same Spark engine, just not spread across a
cluster.

### 1.2 How is Spark different from a "regular" SQL database?

> **TL;DR:** A database is a place where you **store** data. Spark is a tool
> you **run** data through. Spark does not own or store the data; it reads it
> from files or other systems, processes it, and writes results somewhere.

| | Traditional database (PostgreSQL, MySQL…) | Spark |
|---|---|---|
| **What it is** | Storage + query engine + transactions, all in one product | **Only a processing engine**. It doesn't store data itself. |
| **Where data lives** | In the database's own internal files | In external files/systems: Parquet, CSV, S3, HDFS, Delta, other databases… |
| **Schema** | *Schema-on-write*: data must match the table when inserted | Often *schema-on-read*: structure is applied when the data is read |
| **Typical use** | **OLTP**: many small reads/writes ("update this customer's address"), with ACID transactions | **OLAP / batch**: scanning, joining and aggregating huge datasets ("total sales per region over 5 years") |
| **Indexes** | Central (e.g. B-trees for fast lookups) | Basically none; relies on full scans, column formats, partition pruning and statistics |
| **How it grows** | Mostly *scale up* (buy a bigger server) | *Scale out* (add more machines) |
| **When work happens** | Each statement runs immediately | **Lazy**: transformations only build a plan; nothing runs until an *action* such as `count()`, `collect()` or `write` |
| **Recovering from failures** | Logs and replicas | **Lineage**: Spark remembers how each piece of data was computed and simply recomputes lost pieces |

**In our project:** the TPC-DS tool generates `.dat` text files, we convert
them to **Parquet** files, and Spark reads those Parquet files to run the
queries. The data lives in files, never "inside" Spark.

### 1.3 What is Spark's main objective?

> **TL;DR:** To process data that is too big for one computer, quickly, with a
> simple API.

Before Spark, the standard tool was **Hadoop MapReduce**. Its big weakness:
after **every** step it wrote intermediate results to disk, and the next step
read them back. Disk is slow.

Spark's key idea was to **keep intermediate data in memory** whenever possible
and to optimize the whole job as one connected graph of steps (a **DAG**,
Directed Acyclic Graph). This made it 10–100× faster than MapReduce for jobs
with many steps.

### 1.4 Why would someone choose Spark?

> **TL;DR:** Because it scales from a laptop to thousands of machines with the
> same code, and handles SQL, ETL, streaming and machine learning in one tool.

- **Scale:** the same code runs on your laptop or on a 1,000-machine cluster.
- **One engine for many jobs:** SQL, ETL (data cleaning and transformation),
  real-time streaming, machine learning (MLlib), graph processing.
- **Open file formats:** works directly with Parquet, ORC, Delta Lake and
  Iceberg on cheap storage (the "lakehouse" approach).
- **Smart optimizer:** the **Catalyst** optimizer rewrites your query into a
  faster equivalent, and the **Tungsten** engine executes it efficiently.
- **Many languages:** Python, SQL, Scala, Java and R all end up as the same
  optimized plan.

**When *not* to use Spark:** for small data or quick single-row lookups.
Coordinating parallel work has overhead. For a small dataset on a single
laptop, a single-machine engine (for example DuckDB) is often faster. Spark's
overhead pays off when the data is really big.

---

## 2. Java

### 2.1 Why does Spark need Java?

> **TL;DR:** Spark is written in Scala, and Scala runs on the Java Virtual
> Machine (JVM). So Spark itself is a Java program, even when you use it from
> Python.

When you use **PySpark** (Spark from Python, as we do), Python is only a thin
remote control. The real work happens in the JVM:

```
 ┌───────────────────┐   Py4J bridge   ┌──────────────────────────────────┐
 │  Python process   │ ──────────────► │           JVM process            │
 │                   │                 │                                  │
 │ spark.sql("...")  │   "please run   │  parse → optimize → plan →       │
 │                   │    this query"  │  execute → return the result     │
 └───────────────────┘                 └──────────────────────────────────┘
```

The bridge between both is a library called **Py4J**. Our code uses it
directly when it calls the Java garbage collector from Python (see
[`pipeline/benchmark.py`](../pipeline/benchmark.py)):

```python
spark._jvm.System.gc()   # Python → Py4J → runs java.lang.System.gc() in the JVM
```

That is why our [`Dockerfile`](../Dockerfile) installs **both** `pyspark` and
`openjdk-21-jre-headless` (Java 21).

**Why did Spark pick the JVM?** The big-data world it was born into (Hadoop,
HDFS, YARN) was already Java-based. The JVM also offered portability, mature
multi-threading, automatic memory management and a very good runtime compiler.

### 2.2 What is the JVM and why is it necessary?

> **TL;DR:** The JVM (Java Virtual Machine) is a program that runs Java
> *bytecode*. It makes Java programs portable, manages memory automatically,
> and makes code faster while it runs.

When you compile Java (or Scala) code, you don't get machine code for a
specific CPU. You get **bytecode**, a universal intermediate format. The JVM
reads that bytecode and runs it on whatever machine you have.

**Analogy:** bytecode is like a recipe written in a universal language, and
the JVM is a cook who can read that language in any kitchen (Linux, Mac,
Windows, Intel, ARM…).

The JVM has three main jobs:

1. **Load classes:** it loads code (`.class` files) **only when it's first
   needed**.
2. **Execute code:** at first it *interprets* bytecode (slow). Code that runs
   often ("hot" code) gets compiled into fast native machine code by the
   **JIT (Just-In-Time) compiler**. So **Java programs get faster the longer
   they run** (remember this for [Section 4](#4-why-is-the-first-run-of-every-query-slower)).
3. **Manage memory:** objects are created in a memory area called the
   **heap**. When an object is no longer used, the **Garbage Collector (GC)**
   frees it automatically. You never have to free memory by hand. Java 21's
   default garbage collector is called **G1**.

**Why is it necessary?** Because bytecode is not something your CPU can run
directly. The JVM is what turns it into real execution, and it also provides
portability ("write once, run anywhere"), automatic memory management and
runtime optimization.

---

## 3. Spark + Java memory: why we force the garbage collector

> **TL;DR:** The JVM grows its memory as queries run but doesn't give it back
> to the operating system on its own. After many queries, the machine ran out
> of RAM and Linux killed Spark. Forcing a garbage collection between queries,
> plus two JVM settings, makes the JVM return unused memory.

### Our configuration

In [`pipeline/constants.py`](../pipeline/constants.py):

```python
.config("spark.driver.memory", "6g")       # becomes the JVM flag -Xmx6g (max heap = 6 GB)
.config("spark.driver.extraJavaOptions",
        "-XX:MinHeapFreeRatio=10 -XX:MaxHeapFreeRatio=30")
```

In [`pipeline/benchmark.py`](../pipeline/benchmark.py), after each statement
finishes all its runs:

```python
spark._jvm.System.gc()
```

### What was going wrong (step by step)

**Step 1: the heap grows but doesn't shrink.**
The JVM starts with a small heap and asks the operating system for more memory
as queries need it, up to the 6 GB limit. Queries create lots of temporary
objects: hash tables for joins, buffers for aggregations, decoded Parquet
data. When a query ends, those objects become garbage and the GC eventually
cleans them up. **But** the G1 garbage collector usually **keeps the memory it
already has** instead of giving it back, because it expects to need it again
soon.

> **Analogy:** a restaurant that keeps adding tables as more customers come
> in, but never removes them when the customers leave. The floor space stays
> occupied even when the restaurant is empty.

**Step 2: the JVM uses more memory than just the heap.**
Total memory used by the process is roughly:

```
heap (up to 6 GB)
+ off-heap memory (loaded classes, compiled code, thread stacks,
                   network buffers, Parquet decompression buffers…)
+ the Python process
= often 7–8 GB in total
```

**Step 3: Linux kills the process.**
Inside Docker or on a laptop with limited RAM, after many queries the machine
runs out of memory. The Linux **OOM (Out-Of-Memory) killer** then terminates
the biggest process, which is the Spark JVM.

Note that this is **not** a Java `OutOfMemoryError`. From the JVM's point of
view everything was fine (it was still under its 6 GB limit). It's the
operating system that ran out of memory.

### How the fix works

1. **`System.gc()`** asks the JVM for a **full garbage collection**: it frees
   all garbage and compacts the heap.
2. After a full GC, the JVM decides whether to resize the heap, using two
   settings:
   - **`MaxHeapFreeRatio=30`**: "if more than 30% of the heap is free after
     GC, **shrink the heap** and return that memory to the operating system."
   - **`MinHeapFreeRatio=10`**: "if less than 10% is free, grow the heap."
3. Result: between queries the process's memory goes back down, so it never
   builds up enough for the OOM killer to act.

**Good practices already applied:**
- The GC call is **outside the timed section**, so it doesn't distort the
  measured query times.
- A cleaner long-term alternative would be to set `spark.driver.memory` to
  what the container really has, leaving room for off-heap memory. The current
  approach is still a reasonable, well-understood workaround.

> **Side note:** in the config, `"spark.sql.ansi.enable"` is a typo. The
> correct key is `spark.sql.ansi.enabled`. Spark silently ignores unknown
> keys. It's harmless here only because ANSI mode is already on by default in
> Spark 4.x.

---

## 4. Why is the first run of every query slower?

> **TL;DR:** The first run starts "cold": data is read from disk, Spark has to
> generate and compile code for the query, and the JVM hasn't optimized that
> code yet. Runs 2 and 3 reuse all of that work. The garbage collector doesn't
> erase any of it.

### First, when does the GC actually run?

Looking at [`pipeline/benchmark.py`](../pipeline/benchmark.py), the GC is
called **after all 3 runs of a statement**, not between runs:

```
query 1: run 1 → run 2 → run 3 → GC → query 2: run 1 → run 2 → run 3 → GC → ...
```

So runs 2 and 3 never come right after a GC. Only run 1 does.

### Why run 1 is slower

Even if we did run GC between runs, run 1 would still be slower. The GC only
frees **heap objects that are no longer used**, and none of the "warm-up" work
below is stored that way:

| # | Reason | What happens in run 1 | Why runs 2–3 are faster | Does GC erase it? |
|---|---|---|---|---|
| 1 | **OS page cache** (usually the biggest factor) | Parquet files are read from **disk** | Linux keeps recently read file blocks in RAM, so later runs read from **memory** | ❌ No. This cache belongs to the operating system, not the JVM. |
| 2 | **Code generation** | Spark *writes Java code* for the query, compiles it (with the **Janino** compiler) and loads it | Spark keeps the compiled code in a cache and reuses it | ❌ No. The cache is still in use, so the GC keeps it. |
| 3 | **JIT warm-up** | Code starts out *interpreted* (slow), then the JIT compiles hot parts to native code | Runs use fast, already-compiled native code | ❌ No. Compiled code lives outside the heap (in the "code cache"). |
| 4 | **Class loading** | The JVM loads and verifies classes the first time a feature is used (a function, a join type…) | Classes are already loaded | ❌ No. They stay loaded for the JVM's lifetime. |
| 5 | **Metadata caching** | Spark reads Parquet file footers, lists files, looks up tables | That information is cached | ❌ No. |
| 6 | **Heap regrowth** (a side effect of our GC fix) | After the GC shrank the heap, it must **grow again**, which triggers extra small GCs along the way | The heap is already big enough | — This one is *caused* by our GC. |

> **Analogy:** the first time you cook a new recipe you have to find the
> ingredients, read the instructions and figure out the steps. The second time
> everything is already on the counter and you know what to do. Cleaning the
> dirty dishes (the GC) doesn't make you forget the recipe.

### What this means for our benchmark

- **Run 1 is a "cold" measurement**; runs 2 and 3 are "warm."
- Our current `avg_seconds` **mixes cold and warm** runs.
- Common practices are:
  - report cold and warm times **separately**, or
  - treat run 1 as a **warm-up** and average only the remaining runs.

---

## 5. Glossary

| Term | Meaning |
|---|---|
| **Action** | A Spark operation that actually triggers computation (`count()`, `collect()`, `write`). |
| **Bytecode** | The portable, intermediate code produced by compiling Java/Scala; run by the JVM. |
| **Catalyst** | Spark's query optimizer; rewrites queries into faster equivalent plans. |
| **DAG** | Directed Acyclic Graph: the graph of steps Spark builds to compute a result. |
| **Driver** | The Spark process that plans the job and coordinates the executors. |
| **Executor** | A Spark worker that runs tasks on chunks of data. |
| **Garbage Collector (GC)** | The part of the JVM that automatically frees memory that's no longer used. |
| **G1** | The default garbage collector in Java 21 ("Garbage-First"). |
| **Heap** | The memory area where the JVM stores objects. Its maximum size is set with `-Xmx` (in Spark: `spark.driver.memory`). |
| **JIT compiler** | Just-In-Time compiler: turns frequently used bytecode into fast native machine code while the program runs. |
| **JVM** | Java Virtual Machine: the program that runs Java bytecode. |
| **Lazy evaluation** | Spark only builds a plan when you describe transformations, and runs it when an action is called. |
| **OLAP / OLTP** | Analytical workloads (big scans and aggregations) vs. transactional workloads (many small reads/writes). |
| **OOM killer** | The Linux mechanism that kills a process when the system runs out of memory. |
| **Page cache** | RAM that Linux uses to keep recently read file data, so reading it again is fast. |
| **Parquet** | A columnar file format, very efficient for analytical queries. |
| **Partition** | A chunk of a dataset; Spark processes partitions in parallel. |
| **Py4J** | The library that lets Python call Java code; PySpark uses it to talk to the JVM. |
| **Task** | The unit of work Spark runs on one partition. |
| **Tungsten** | Spark's execution engine, focused on efficient memory use and generating code for queries. |

---

## 6. References

### Spark

- Apache Spark. *Official documentation.* <https://spark.apache.org/docs/latest/>
- Apache Spark. *Cluster Mode Overview* (driver, executors, tasks). <https://spark.apache.org/docs/latest/cluster-overview.html>
- Apache Spark. *Spark SQL, DataFrames and Datasets Guide.* <https://spark.apache.org/docs/latest/sql-programming-guide.html>
- Apache Spark. *Configuration* (`spark.driver.memory`, `spark.driver.extraJavaOptions`). <https://spark.apache.org/docs/latest/configuration.html>
- Apache Spark. *Tuning Spark* (memory management and GC tuning). <https://spark.apache.org/docs/latest/tuning.html>
- Apache Spark. *ANSI Compliance* (`spark.sql.ansi.enabled`). <https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html>
- Zaharia, M. et al. (2012). *Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing.* NSDI '12. <https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/zaharia>
- Armbrust, M. et al. (2015). *Spark SQL: Relational Data Processing in Spark.* SIGMOD '15. <https://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf>
- Databricks. *Project Tungsten: Bringing Apache Spark Closer to Bare Metal.* <https://www.databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html>
- Databricks. *Apache Spark as a Compiler: Joining a Billion Rows per Second on a Laptop* (whole-stage code generation). <https://www.databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html>
- Py4J. *A Bridge between Python and Java.* <https://www.py4j.org/>
- Janino. *A super-small, super-fast Java compiler* (used by Spark's code generation). <https://janino-compiler.github.io/janino/>

### Java and the JVM

- Oracle. *The Java Virtual Machine Specification, Java SE 21.* <https://docs.oracle.com/javase/specs/jvms/se21/html/index.html>
- Oracle. *The `java` Command* (`-Xmx`, `-XX:MinHeapFreeRatio`, `-XX:MaxHeapFreeRatio`). <https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html>
- Oracle. *Java 21 `System.gc()` API.* <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/System.html#gc()>
- Oracle. *HotSpot Virtual Machine Garbage Collection Tuning Guide, Java 21.* <https://docs.oracle.com/en/java/javase/21/gctuning/>
- Oracle. *Garbage-First (G1) Garbage Collector Tuning, Java 21.* <https://docs.oracle.com/en/java/javase/21/gctuning/garbage-first-garbage-collector-tuning.html>
- OpenJDK. *JEP 346: Promptly Return Unused Committed Memory from G1.* <https://openjdk.org/jeps/346>

### Operating system

- The Linux Kernel documentation. *Concepts overview* (page cache, OOM killer). <https://www.kernel.org/doc/html/latest/admin-guide/mm/concepts.html>
