## Extended Experiments

---

### Priority Analysis

We compare with different priority assignments on the search time and workload completion time.

As show in Figure 1, R-Priority assigns priorities based on read frequency, and W-Priority does so based on write frequency. Higher read frequency results in higher priority, whereas higher write frequency leads to lower priority. Rand-Priority assigns priorities randomly, and Eval-Priority determines priorities according to the benefit estimated by the evaluation model.

The results show that \\dbname achieves the shortest search time to reach a similar completion time, whereas the other methods require substantially more time. Although Eval-Priority uses model-based priorities that are more accurate, its heavy reliance on the evaluation model incurs significant overhead, resulting in much longer search time. Overall, our priority strategies are able to identify high-quality configurations within a relatively short time, and all priority methods eventually converge.

*Figure 1 (originally Figure \\ref{expr:cover\_abl\_search\_completion} from LaTeX) is a visual comparison described in the text:*

**Figure 1: Comparison between Different Priority Assignments.**

---

### Sensitivity Analysis

We compare how different parameter settings affect the behavior of the workload-window mechanism.

As shown in Table 1, we report the number of triggered searches, the number of reconfigurations, and the average throughput and latency normalized by the performance under the default setting.

The results show a smaller \$ws\$ significantly increases the number of searches but only slightly increases reconfigurations, as layout changes are applied only when the estimated benefit exceeds the cost.

In contrast, a larger \$ws\$ reduces the search frequency, ultimately resulting in worse performance.

A similar trend is observed for \$\\delta\$: higher thresholds make \\dbname less sensitive to workload changes, leading to fewer searches and degraded responsiveness.

**Table 1: Sensitivity Analysis for Incremental Adjustment.**


| **Method (ws=1min, δ=24% By Default)** | **Search Count** | **Transformation Count** | **Average TPS Norm.** | **Average Latency Norm.** |
| --------------------------------------- | ---------------- | ------------------------ | --------------------- | ------------------------- |
| Default                                 | 8                | 4                        | 1                     | 1                         |
| **\$ws\$**=20s                          | 19               | 6                        | 0.97                  | 1.05                      |
| **\$ws\$**=10min                        | 3                | 2                        | 0.96                  | 1.34                      |
| **\$\\delta\$**=10%                     | 29               | 4                        | 0.98                  | 1.02                      |
| **\$\\delta\$**=50%                     | 1                | 1                        | 0.96                  | 1.26                      |

---

## Discussion on Different Architectures

---

### Generality

Jasper is generalizable beyond TiDB.

Jasper’s core contribution lies in its joint adaptive storage configuration and synchronization-aware evaluation model, which is prominent in separated row-column storage systems.

However, the underlying logic is inherently architecture-agnostic.

For integrated row–column storage systems such as OceanBase \\cite{oceanbase}, it extends its storage engine to support columnar storage in v4.3. This enhancement enables a unified row–column storage architecture within a single OBServer node, implemented under a single framework.

The data is divided into the baseline data (on disk) and incremental data (row format in memory) in one OBServer node.

Users can specify the hybrid row-column storage for the baseline data. This achieves a balance between the performance of TP workload and that of AP queries, but incur issues like data synchronization.

When a query requires the freshest data, the system should read the incremental data and merge it with the existing baseline data to produce the final result.

A better approach is to design a fine-grained column store configuration that excludes write-heavy data and minimizes unnecessary data-merge operations.

Jasper can jointly configure partitions and column store replicas, enabling it to balance mixed workloads while reducing the overhead of data synchronization.

Therefore, \\dbname can be generalized to the integrated row–column storage systems.

As explained in our previous response, we have implemented Jasper’s core mechanisms in OceanBase.

The experiment results show that \\dbname also perform well in this integrated row–column architecture.

Systems like Oracle \\cite{lahiri2015oracle} and SQL Server \\cite{larson2015real} adopt a unified storage worker like disk-based row store and in-memory column store.

In this schema, we can apply Jasper’s core idea to jointly determine the optimal partitioning strategy and select frequently read partitions to be loaded into the in-memory column store.

As mentioned in Oracle \\cite{lahiri2015oracle}: \`\`The contents of the IM column store are built from the persistent contents within the row store, a mechanism referred to as populate. It is possible to selectively populate the IM column store with a chosen subset of the database.''

Oracle supports applying the IM column store to multiple levels of granularity—tablespaces, tables, entire partitions, or even specific sub-partitions—perfectly, aligning with our fine-grained selective column store approach.

Because Oracle is not open-source, we cannot integrate our evaluation model with its optimizer and therefore cannot run experiments to directly validate our claims.

However, according to Oracle’s official documentation \\cite{Oracle}, “Oracle Database In-Memory transparently accelerates analytic queries by orders of magnitude, enabling real-time decisions.” This suggests that our approach should likewise be effective in architectures combining a disk-based row store with an in-memory column store.

We evaluate the generality of our approach by applying it to OceanBase v4.3.5 \\cite{oceanbase}.

OceanBase achieves its HTAP capability by introducing column store.

We leverage MCTS-HTAP search methodology in OceanBase to explore efficient partitions and selective column store.

We measure the completion (execution) time to complete the OLTP-Heavy, Balanced, and OLAP-Heavy CH-benchmark workloads following the setup in Section \\ref{6.2}.

Figure 2(a) compares the workload completion time. Jasper-OB consistently achieves the shortest completion time—reducing time by 15.8% to 39.8%—confirming the joint adaptive optimization effectively mitigates synchronization costs even when adapted to a different HTAP systems.

In contrast, other methods lack an efficient optimization strategy for partitioning and columnar storage.

This result suggests that Jasper is also applicable to HTAP systems with other architectures.

---

### Comparison with Other Systems

We conduct comparison experiments with several HTAP databases in different architectures, including OceanBase-Hybrid \\cite{oceanbase}, Ocean-Base-Column \\cite{oceanbase}, SingleStore \\cite{SingleStore}, and Umbra \\cite{kemper2011hyper, schmidt2024two}.

OceanBase organizes data into baseline SSTables and an in-memory MemTable.

In OceanBase-Column, SSTables are stored only in columnar format,

whereas OceanBase-Hybrid maintains both row- and column-format SSTables for the same table.

SingleStore, the successor to MemSQL, stores all data in column format while maintaining in-memory row indexes for transactional access.

Umbra stores data primarily in a columnar format, and in its latest version \\cite{schmidt2024two}, it keeps a portion of hot data in a row store.

We also examined Hyrise \\cite{grund2010hyrise}, an in-memory column store database, but excluded it from the evaluation due to its lack of support for complex join queries in the CH-benchmark.

Since Umbra is a single-node system, we deploy all systems in single-node mode for a fair comparison.

As TiDB’s storage layer relies on Raft and cannot run standalone, we implement Jasper on OceanBase (Jasper-OB) for this experiment.

We compare workload completion time and throughput across all systems using the Balanced CH-benchmark under the same conditions as Section \\ref{6.2}.

As shown in Figure 2(b), Jasper-OB delivers the best overall performance, completing the workload much faster than SingleStore and OB-Hybrid due to its adaptive storage strategy. Jasper-OB and OB-Hybrid also achieve the highest throughput, benefiting from OceanBase’s efficient LSM-tree design. Umbra finishes the workload quickly but has low OLTP throughput because of its columnar storage, and OB-Column shows similarly limited throughput, as column stores struggle under highly concurrent transactional workloads.

*Figure 2 (originally Figure \\ref{expr:execution\_time\_ob} and \\ref{expr:exec\_tps2} from LaTeX) is a performance comparison:*

**Figure 2: Performance Comparison among Different Architectures.**

---

## Cost Functions

Table 2 presents a comprehensive summary of the cost functions formulated for key operators. The cost functions rely on a set of tunable parameters to capture the performance characteristics of the underlying storage engine and target deployed system.

To accurately estimate the execution overhead, our model incorporates both query-specific statistics—such as cardinality (rows) and tuple width (rowSize)—and system-specific performance coefficients (e.g., scanFactor, cpuFactor, and memFactor).

By separating system-specific coefficients from query-specific statistics, the cost functions provide a flexible mechanism to quantify resource consumption across distinct operator types, ranging from I/O-intensive scans to CPU-intensive joins.

**Table 2: Cost Function of Operators.** The execution overhead of operators on different store is calculated using different Factor parameters.


| **Operator**    | **Cost Function**                                                                                                                                                                                                                                                                                      |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| TableScan       | rows\* log2(rowSize) \* scanFactor + c \* log2(rowSize) \* scanFactor)                                                                                                                                                                                                                                 |
| IndexScan       | rows\* log2(rowSize) \* scanFactor                                                                                                                                                                                                                                                                     |
| Selection       | row\* cpufactor \* numFuncs                                                                                                                                                                                                                                                                            |
| Update & Insert | rows\* rowSize \* netFactor                                                                                                                                                                                                                                                                            |
| Sort            | rows\* log2(rows) \* len(sort-items) \* cpu-factor + memQuota \* memFactor + rows \* rowSize \* diskFactor                                                                                                                                                                                             |
| HashAgg         | rows\* len(aggFuncs) \* cpuFactor + rows \* numFuncs \* cpuFactor + (buildRows \* nKeys \* cpuFactor + buildRows \* buildRowSize \* memFactor + buildRows \* cpuFactor + probeRows \* nKeys \* cpuFactor + probeRows \* cpuFactor) / concurrency                                                       |
| SortMergeJoin   | leftRows\* leftRowSize \* cpuFactor + rightRows \* rightRowSize \* cpuFactor + leftrows*numFuncs*cpuFactor + rightrows*numFuncs*cpuFactor                                                                                                                                                              |
| HashJoin        | buildRows\* buildFilters \* cpuFactor + buildRows \* nKeys \* cpuFactor + buildRows \* buildRowSize \* memFactor + buildRows \* cpuFactor + (probeRows \* probeFilters \* cpuFactor + probeRows \* nKeys \* cpuFactor + probeRows \* probeRowSize \* memFactor + probeRows \* cpuFactor) / concurrency |

---

## Online Reconfiguration

The interaction between transformations and concurrently running queries/transactions is managed through an online reconfiguration process.

1. **Non-blocking copy phase.** When **\\dbname** determines a new layout (**\$L\_{new}\$**), the system initiates the creation of this layout in the background. Data from the existing layout (**\$L\_{old}\$**) is asynchronously copied to **\$L\_{new}\$** without blocking OLTP or OLAP traffic on **\$L\_{old}\$**.
2. **Concurrency Maintenance.** While the copy is in progress, new updates (writes) continue to be applied to **\$L\_{old}\$**. These updates are simultaneously tracked and applied to **\$L\_{new}\$** via the Raft logs, ensuring **\$L\_{new}\$** stays current.
3. **Atomic Switch.** Once **\$L\_{new}\$** is fully caught up with **\$L\_{old}\$** (i.e., they are consistent up to a certain commit timestamp), the system performs a rapid, atomic metadata update. This switch redirects the logical view of the data segment from **\$L\_{old}\$** to **\$L\_{new}\$**.

This guarantees that ongoing queries always observe a consistent snapshot throughout the entire transformation process.

This process is similar to online schema evolution in TiDB \\cite{Online}.
