# Cassandra：Read-your-writes 实验

## 目标与文件

验证同一个客户端成功写入数据后，接着读取时是否能看到自己的写入。每轮使用一个独立 key，先将版本 0 写到全部副本，再写版本 1 并立即读取。

| 文件 | 用途 |
|---|---|
| `read_your_writes.py` | 唯一实验实现：正常运行、节点故障、网络分区、记录与汇总 |
| `ryw_experiments.py` | 批量运行三个场景，每组 100 轮、每个场景重复 2 次 |
| `docker-compose.yml` | 部署三个节点，客户端端口为 10001、10002、10003 |
| `Dockerfile.cassandra` | 给 node3 安装 iptables，配合 Compose 的 NET_ADMIN 权限进行网络分区 |
| `environment.yml` | Python 3.12、cassandra-driver 3.30.1、pyasyncore 1.0.5 |

## 1. 准备环境

以下命令在项目目录的 PowerShell 中运行。先打开 Docker Desktop，使用 Linux containers。

使用当前项目的部署配置启动集群：

```powershell
docker compose up -d --build
docker compose ps
docker exec cassandra-1 nodetool status
```

等三个容器健康、三个节点都是 `UN` 再实验。`--build` 用于构建包含 iptables 的 node3 镜像；仅更新文件不会更新已运行容器。已有命名数据卷继续使用。三个节点均使用 Cassandra 5.0.9。

确认 node3 的工具和权限（只读检查，不会断网）：

```powershell
docker exec --user root cassandra-3 iptables -S
```

有 `.venv` 时直接使用下方命令。新电脑可用 Conda：

```powershell
conda env create -f environment.yml
conda activate DSA5208_Project1
```

使用 Conda 时，将下面的 `.venv/Scripts/python.exe` 换成 `python`。实验使用 `ryw_matrix.samples`，复制因子 RF=3，不修改组员的产品表。

已有同名 Conda 环境时，可更新依赖：

```powershell
conda env update -n DSA5208_Project1 -f environment.yml
conda activate DSA5208_Project1
```

`pyasyncore` 提供 Python 3.12 所需的 asyncore 兼容模块。Docker Desktop、Compose 和容器内的 iptables 不在 Conda 环境中安装，由 Docker 部署提供。

## 2. 运行实验

默认只测正常运行，每组 30 轮；不再自动插入关机演示。默认比较六组配置：ONE/ONE、ONE/QUORUM、QUORUM/ONE、QUORUM/QUORUM、ALL/ONE、ONE/ALL（写级别/读级别）。

先跑每组 3 轮，检查流程：

```powershell
.venv/Scripts/python.exe read_your_writes.py --iterations 3
```

正式采样示例：

```powershell
.venv/Scripts/python.exe read_your_writes.py --scenario normal --iterations 100
.venv/Scripts/python.exe read_your_writes.py --scenario node_failure --iterations 30
.venv/Scripts/python.exe read_your_writes.py --scenario partition --iterations 30
```

三条命令分别运行，不要同时运行；故障实验期间其他组员也不要使用集群。

只测一组时，同时指定读写级别：

```powershell
.venv/Scripts/python.exe read_your_writes.py --scenario normal --iterations 100 --write-cl QUORUM --read-cl QUORUM
```

| 场景 | 做什么 | 读写入口 |
|---|---|---|
| normal | 三个节点正常，逐轮写入版本 1 后读取 | node1 写，node2/node3 交替读 |
| node_failure | 所有 key 初始化后停止 node3，确认离线后测试，结束后恢复 | node1 写，node2 读 |
| partition | 初始化后阻断 node3 的 TCP 7000/7001，保留客户端 9042，确认分区后测试并恢复 | node1 写，node3 读 |

“入口”指协调节点，数据库仍按一致性级别访问副本。同一客户端 A 串行控制这些连接。

分区在 node3 内通过 `docker exec --user root ... iptables` 添加 `CMDD_RYW` 独立规则链。节点仍运行，但 node3 收不到新写入；其客户端端口仍可连接。不要同时手工添加其他断网规则。

### 批量运行

```powershell
.venv/Scripts/python.exe ryw_experiments.py
```

批量脚本依次运行 `normal`、`node_failure`、`partition`，每个场景重复 2 次，每次六组配置、每组 100 轮。全部成功时共 6 次运行、3600 条试验记录；每次成功运行后等待 5 秒。修改脚本顶部的 `SCENARIOS`、`ITERATIONS`、`REPEATS` 可调整规模。

当前脚本的子进程解释器固定为 `.venv/Scripts/python.exe`，必须在项目根目录运行并存在该环境。仅使用 Conda 时，请使用上面的单次实验命令；即使用 `python ryw_experiments.py` 启动，子进程仍会使用 `.venv`。

某次运行返回非零退出码时，仅跳过当前场景的剩余重复，随后继续下一个场景。末尾的 `All experiments finished!` 不代表全部成功，应逐个检查结果状态和恢复记录。

## 3. 结果怎么读？

每次运行打印一个独立的 `results/<scenario>_<UUID>` 目录：

- `trials.csv`：一行是一次实际执行的写入或读取操作，列顺序见下表。同一轮写后读共享 `trial_id`；写入失败时不执行读取，因此只有一行。初始化版本 0 的操作不计入此表。
- `summary.csv`：每种配置的轮数、有效读取数、违反次数、违反率和失败次数，可直接用 Excel 打开。
- `metadata.json`：实际软件版本、部署配置文本、运行状态、故障视图和恢复结果。配置文本不代表运行容器已经完成重建。

| 字段 | 含义 |
|---|---|
| `trial_id` | 运行 UUID、写级别、读级别、组内轮次组成的标识 |
| `model` | 固定为 `RYW`，表示 Read-your-writes 一致性模型 |
| `scenario` | `normal`、`node_failure` 或 `partition` |
| `client` | 当前固定为 `A` |
| `operation` | `WRITE` 或 `READ` |
| `key` | 本轮数据的 UUID 主键 |
| `version_written` | 本轮尝试写入的版本，当前为 1；读行保留此值用于比较 |
| `version_observed` | 实际读到的版本；写操作、读取失败或空记录时留空 |
| `read_cl` | 本轮的读取一致性级别 |
| `write_cl` | 本轮的写入一致性级别 |
| `target_node` | 此次请求的协调节点，如 `node1`；不是所有参与副本 |
| `success` | 此次请求是否成功，`True` / `False`；旧值或空记录也是成功读取 |
| `violation` | 成功读取时判断是否违反 RYW；写操作和失败请求留空 |
| `latency_ms` | 客户端测得的单次请求耗时（毫秒），包括失败请求 |
| `timestamp` | 操作开始时间，ISO 8601 格式，含时区 |
| `error` | 失败时的异常类型和信息，成功时留空 |

新的明细不再输出 `outcome`，下列分类用于解释汇总统计：

| 分类 | 意思 |
|---|---|
| PASS | 写入确认后，成功读取到版本 1 或更新值 |
| VIOLATION | 写入确认后，成功读取却得到旧值或空记录 |
| WRITE_FAILED | 写入未确认，跳过本轮读取；超时并不证明写入没有生效 |
| READ_FAILED | 写入确认，但读取失败；不算 RYW 违反 |

违反率 = `violations / valid_reads`，有效读取只包括 PASS 与 VIOLATION。没有有效读取时留空，不能写成 0%。退出码 0 表示采样流程完成，不代表没有违反或请求失败；应检查 summary.csv。

正式报告引用 `status=completed` 的完整运行；故障场景还要确认 recovery 已验证。中止时保留已有样本与状态，不把它当成完整实验。

`summary.csv` 仍按写后读轮次汇总，`trials` 为写操作数，不是明细行数。批量运行的 3600 轮最多产生 7200 行操作记录。新格式在 metadata 中标记 `schema_version=2`、`row_grain=operation`；已有历史 CSV 保持原格式。

## 4. 预测与解释

RF=3 时 ONE、QUORUM、ALL 分别需要 1、2、3 个副本回应。单写入者、无 TTL/删除、固定拓扑、递增写入时间戳下，R+W>3 的组合有副本交集，预期成功读取满足 RYW。因此 QUORUM/QUORUM、ALL/ONE、ONE/ALL 有交集保证，其余三组不保证。

正常环境下弱配置也可能没有旧读。分区后在少数侧读取时，ONE/ONE 和 QUORUM/ONE 预期能展示旧读；需要 QUORUM 或 ALL 的读取预计失败；ALL 写入预计失败。故障或分区中的错误需作为可用性结果单独统计。

每轮 key 独立，版本 0 与版本 1 使用明确递增的时间戳。这个受控实验不覆盖并发写入、客户端时钟异常或全部故障布局。没有观察到违反不等于证明保证。

## 5. 恢复与旧命令变化

普通异常会进入自动恢复流程；强制结束进程或关机后，手动恢复：

```powershell
.venv/Scripts/python.exe read_your_writes.py --recover
```

该命令启动 node3，仅删除本实验的 CMDD_RYW 规则，检查三个节点 UN 且可连接。仅关节点而旧镜像没有 iptables 时，可先 `docker start cassandra-3`，再检查集群。其他实验添加的网络规则不在本脚本恢复范围内。

旧参数 `--normal-only` 已取消，改用 `--scenario normal`（也是默认值）；`--error-sample` 已取消，使用 `--scenario node_failure`。旧版测试“写后关节点”，现在故障场景是“先故障再测写后读”。新旧数据的 key、行结构和操作顺序不同，不能直接混合统计。

## 验证状态与提交

截至 2026-09-16，当前 `results/` 中保留了三个场景各 3 次运行的记录。各次 `metadata.json` 均标记为 `completed`，每组配置 100 轮；节点故障和分区场景的恢复字段均记录为 `verified: three UN nodes and CQL reachable`。这些是已有运行记录，本次文档更新未重新执行集群实验。批量脚本当前重复次数为 2，与历史记录数量无须相同。

检查依赖导入和命令行入口（不会启动实验）：

```powershell
.venv/Scripts/python.exe -c "import cassandra, asyncore; print(cassandra.__version__)"
.venv/Scripts/python.exe read_your_writes.py --help
```
