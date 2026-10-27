# Arbor

**结合时间账户图与入口森林的区块链分片账户分配。**

仓库提供模块化方法实现、原生执行引擎、冻结模型、数据处理、训练与论文实验配方。[English](README.md)

## 两条命令跑通示例

支持 Linux／WSL2，推荐 Ubuntu 24.04，需要 Python **3.12**、C++17 编译器和 Go **1.22+**。Linux x86_64 缺少 Go 时，安装器将校验后的固定工具链放入仓库 `.tools/`，不修改系统环境。

私有预览期间，请先取得仓库访问权限，安装 [GitHub CLI](https://cli.github.com/)，并执行 `gh auth login` 和 `gh auth setup-git`，再克隆仓库。

```bash
sudo apt-get update
sudo apt-get install -y python3.12 python3.12-venv g++ git
git clone https://github.com/JiaoTang666/arbor.git
cd arbor
./install.sh
./arbor run demo
```

示例自动生成历史与测试数据，构建账户图、入口森林、初始布局，再串行执行 Arbor、Monoxide、动态 CLPA。每种方法使用真实 PBFT 节点完成 120 笔交易，覆盖已知账户、伙伴跟随与联合接入。示例用于验证软件流程，不作为论文性能结果。

终端显示配置、阶段、接入／完成数量、耗时和最终性能表。原生日志写入运行目录。Ctrl+C 退出显示后，服务器队列继续运行，不依赖浏览器或 Windows 开机。

## 论文复现

```bash
./arbor data paper
./arbor run full
./arbor run ablation --cleanup-db
./arbor run halflife --cleanup-db
./arbor run group-size --cleanup-db
./arbor run tolerance --cleanup-db
./arbor run scale --cleanup-db
```

当前仓库和数据发布为私有。获授权的复现者使用 GitHub CLI 正常登录（`gh auth login`）后下载；程序不读取浏览器凭据。下载包与解包成员均检查 SHA256，大数据不进入 Git。

| 命令 | 运行内容 |
|---|---|
| `full` | 单次 Arbor，16 片、2,000 笔/秒 |
| `ablation` | 完整方法与四个正式组件消融，共 5 组 |
| `halflife` | 历史半衰期 3×3×3 网格，共 27 组 |
| `group-size` | 分组上限 16／32／64 |
| `tolerance` | 负担容忍度 1.05／1.10／1.20 |
| `scale` | 三种方法 × 4／8／12／16 片 × 50／75／100% 负载，共 36 组 |

论文默认历史尺度 **6h／48h／3.5d**、分组 **64**、容忍度 **1.10**。在线时间核仍为 **6h／24h／7d**。每片 4 节点，批量／区块容量 1,000，间隔 5 秒；100% 名义注入负载为 `200 × 分片数` 笔原始交易/秒。三种方法使用同一个新构建的执行引擎。实验语义与参考数据见 [实验说明](docs/experiments.md)。

## 配置与运行管理

```bash
./arbor run full --method Monoxide --shards 8 --rate 1200 --dry-run
./arbor run full --config my-run.yaml --detach
./arbor doctor
./arbor status runs/<运行编号>
./arbor resume runs/<运行编号>
./arbor results runs/<运行编号> --plot
```

参数优先级为命令行、YAML、默认值。`--dry-run` 展示任务及生效配置，不启动节点；单组覆盖使用 `full`，矩阵配方按自身网格展开。`--json`、`--no-color` 适合脚本和非交互终端。

**默认保留数据库。** `--keep-db` 与 `--cleanup-db` 互斥。只有当前组成功完成、进程退出、两份服务器结果小包核验通过后，显式清理才删除本组数据库与原生大日志。输入、共享布局、旧结果和失败现场保留；两份目录副本位于同一服务器。

每个运行目录只允许一个控制器。续跑跳过完成组，接续未启动组；失败或中断的原生组不会自动重跑。确认需要重试时，检查失败原因并显式新建运行。运行状态、完整配置、环境与构建标识保存在目录中。

结果包括 `summary.json/CSV`、逐组 `performance.json`、配置、进程退出和清理回执；`--plot` 生成矢量 PDF、PNG 与灰度预览。完成延迟从计划释放计时，TPS 包含注入及排空，P95/P99、跨片率、排空、逐片工作一并保留。每组要求全输入完成、未完成为零和阶段守恒。

## 数据与训练

```bash
./arbor train fusion --small
./arbor train admission --small
./arbor train fusion --dataset data/paper
./arbor train admission --dataset data/paper
```

训练从历史前缀生成样本，再拟合导出；小样例用于软件验收。正常实验始终使用冻结模型，不隐式训练，也不覆盖模型。原始 XBlock 清洗、切分、字段与模型来源见 [数据说明](docs/data-models.md)。

## 资源与常见问题

示例准入为 2 GiB 可用内存和数据／系统盘各 2 GiB 余量。论文配方沿用 300 GiB 可用内存、数据盘 70 GiB、系统盘 100 GiB 余量，离线准备受 8 GiB RSS 保护。这些是可配置准入值，不代表实测最低硬件要求。运行器每 5 秒检查资源，只管理自身子进程。较小数据可通过 YAML 的 `resources` 配置更小限额。

运行时清除父环境中的 `GOMAXPROCS`、`GOMEMLIMIT`、`BLOCK_EMULATOR_STATE_DB_TABLE_MB`，避免环境漂移；数据库缓存／句柄／Trie 缓存保持 128 MiB／256／64 MiB。

- 缺少虚拟环境模块：安装 `python3.12-venv`。
- Go 默认依赖站点超时：使用自己的网络代理，或执行 `GOPROXY=https://goproxy.cn,direct ./install.sh`；依赖校验保持开启。
- 下载无权限：检查 `gh auth status` 和私有仓库权限。
- 资源不足：查看 `doctor` 和配置；程序不会静默减少规模。
- 端口冲突：检查占用归属，不终止其他任务。
- 断开终端：先用 `status` 查看，控制器仍在运行时不要重新启动。

验证命令、模块关系、许可与上游声明分别见 [架构说明](docs/architecture.md)、[MIT LICENSE](LICENSE) 和 [第三方声明](THIRD_PARTY_NOTICES.md)。
