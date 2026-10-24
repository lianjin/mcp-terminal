# MCP Terminal：终端会话 MCP Server

本项目提供可由 MCP 客户端或 Agent 持续控制的 **POSIX PTY 终端会话**，而不是单次 shell 命令包装。当前服务版本为 **0.3.0**，传输方式为 **stdio**，不提供 HTTP、SSH 或公网服务。

使用 **MCP Inspector** 保持连接进行人工操作，使用统一验收脚本验证多个会话、增量输出、失败响应与安全策略。以下安装、启动和测试命令均在项目根执行，不依赖预置的机器路径或用户名。

## 1. 已实现功能

服务提供 `ping`、`start_session`、`write_input`、`read_output`、`get_status`、`terminate_session` 六个工具。创建会话后返回独立 ID，进程与 REPL 状态持续保留，后续输入可以使用先前定义的变量。

每个会话具有独立 PTY、进程组、工作目录和环境。后台异步任务持续采集输出、监听退出；启动检查与审计文件 I/O 放到后台线程，PTY 输入/输出等待使用异步就绪通知。一个会话的长任务、输入等待或终止宽限期不应占住其他会话的请求处理。

输出按 UTF-8 增量解码，由有界 ring buffer 保留最新内容。客户端用绝对字符 cursor 分页；读取不消费数据，重复同一 cursor 可以重放。旧内容超出窗口时丢弃，并返回明确的恢复位置。进程结束后关闭 PTY、回收进程，元数据和剩余窗口默认保留 600 秒，到期从内存清除。

安全措施包括最小环境、用户批准的 env 键、启动 cwd 白名单、实际 OS 身份和 JSONL 审计。成功及业务错误使用统一结构，便于 Agent 组合调用与处理失败。

## 2. 环境、安装与启动

要求 Python **3.11+** 和 POSIX PTY；当前实现不提供 Windows ConPTY。运行依赖为官方 `mcp` SDK，开发依赖为 `pytest`、`pytest-asyncio`、`ruff`。项目使用 `src/` 布局，依赖声明见 `pyproject.toml`，锁定版本见 `uv.lock`。

下载仓库后完整验收，要求已安装 Git、Python 3.11+ 和 `uv`，并具有仓库访问权限。下载 ZIP 的用户进入解压后的项目根目录，从创建虚拟环境这一步开始：

```bash
git clone git@github.com:lianjin/mcp-terminal.git
cd mcp-terminal
python3 -m venv .venv
uv sync --locked
.venv/bin/python outputs/validation/run_verification.py
```

仓库只保留 README、源码、测试、默认安全配置、依赖声明/锁文件以及测试所需客户端脚本；不包含虚拟环境、运行日志、测试结果、机器绑定客户端配置或诊断材料。运行结果由脚本重新生成，无需复制任何已有环境。

已有虚拟环境时无需反复重建，可检查包是否安装：

```bash
.venv/bin/python -c "import mcp_terminal; print('import OK')"
```

推荐显式加载安全配置：

```bash
.venv/bin/python -m mcp_terminal --config server-config.json
```

console 入口等价：

```bash
.venv/bin/mcp-terminal --config server-config.json
```

直接运行后安静地等待输入是正常行为：服务等待的是 MCP stdio 协议，不是人工输入 shell 命令的终端。stdout 只用于 MCP 协议，普通日志写 stderr；可用 Ctrl+C 停止。

不带 `--config` 也能启动，但默认仅允许服务启动 cwd 及其子目录，日志写入该 cwd 的 `logs/`，额外 env 键仍默认拒绝。客户端的启动工作目录必须与其解释器及配置路径对应；相对路径示例要求从项目根启动客户端。

## 3. MCP 工具与统一返回

工具业务结果都包含 `ok / data / error`。成功示例：

```json
{"ok":true,"data":{"status":"ok","message":"pong"},"error":null}
```

错误示例：

```json
{"ok":false,"data":null,"error":{"code":"SESSION_NOT_FOUND","message":"会话不存在或保留期已结束。","details":{}}}
```

MCP `structured_content` 与唯一文本块中的 JSON 一致，`isError` 与 `!ok` 一致。这里指工具的业务结果，损坏的 JSON-RPC 等协议层错误仍由 SDK 处理。参数严格校验，不接受额外字段，不把字符串 `"true"` 自动当作布尔值。

### `ping({})`

无参数，返回 `data.status="ok"`、`data.message="pong"`。用于检查连通性，以及业务失败后连接是否仍可用。

### `start_session({command, cwd?, env?})`

`command` 是非空字符串，最多 8192 个字符，由 `shlex.split` 分成可执行文件和参数。**不隐式经过 shell**，`|`、`>`、`&&`、变量展开和命令替换不会自动生效。需要 shell 语义时，调用方必须明确选择 shell 并承担相应风险。

`cwd` 必须是允许根内的现有目录，不传时使用第一个允许根。`env` 只能包含用户事先批准的键。成功返回 `session_id` 与状态快照，包含 PID、命令、cwd、实际用户等；保存 `data.session_id` 后操作该会话。

```json
{"command":"python3 -i -q"}
```

### `write_input({session_id, input, append_newline?})`

`input` 是非空文本，默认原样写入，不解析反斜杠转义。`append_newline` 默认 `false`，设为 `true` 会追加一个真正的 LF，等同于向 REPL 发送 Enter。含追加换行在内，单次最多 65536 个 UTF-8 字节，成功返回 `bytes_written`。

```json
{"session_id":"复制的ID","input":"x = 100","append_newline":true}
```

成功只表示内容提交给 PTY，不表示程序已经执行完。部分写入后超时或审计失败，`error.details.bytes_written` 说明已提交字节，不能盲目重试，否则可能重复执行。输入按会话序列化，不同会话不共享输入。

### `read_output({session_id, cursor?, max_chars?})`

立即返回当前窗口内输出，**不等待新输出、不消费数据**。cursor 是 Python 字符串意义的 Unicode 字符绝对偏移，不是字节偏移；CR/LF 和 ANSI 控制序列也计入原始文本。不要改写输出后自行计算 cursor，应使用 `data.next_cursor`。

`max_chars` 默认 16384，范围 1..65536。返回 `output / cursor / next_cursor / earliest_cursor / latest_cursor / has_more / status / output_complete`。不传 cursor 时从当前最早窗口读取，可能重复返回先前读过的内容。

```json
{"session_id":"复制的ID","cursor":0,"max_chars":4096}
```

读一页后保存 `next_cursor`；有 `has_more` 就继续，否则客户端短暂等待后轮询。没有新输出时返回空字符串，cursor 不推进。按 `next_cursor` 递进读取应无 overlap，主动重复同一 cursor 则允许重放。

旧 cursor 被覆盖时返回 `CURSOR_EXPIRED`，`error.details` 提供 `earliest_cursor / latest_cursor`。从 earliest 恢复，已丢弃内容无法补回。cursor 大于最新偏移时返回 `CURSOR_AHEAD`。

### `get_status({session_id})`

返回后台更新的状态，包含命令/argv、cwd、PID、退出码与原因、UTC 创建/结束/过期时间、窗口范围及缓存/丢弃字符数，以及 `user / uid / gid / duration_seconds / audit_error`。运行时 duration 递增，终态固定。

`running` 表示仍在运行；`exited` 为自然退出，**包括非零退出码和自然信号退出**；`terminated` 为调用者主动终止；`failed` 为已创建会话的 PTY/采集等基础设施问题。启动失败直接返回错误，没有可操作的成功会话 ID。`ok=true` 只说明查询成功，不代表命令成功，应另看 exit_code。

进程已经退出时，尾部输出可能仍在采集。需要完整输出时，应同时等待 `output_complete=true`；若 reason 含 `output_drain_timeout`，尾部采集达到期限，可能有未采集内容。

### `terminate_session({session_id})`

向会话进程组发 SIGTERM，默认给 1 秒宽限；未退出则发 SIGKILL。等待回收和尾部采集后返回最终状态，其他会话仍可被操作。保留期内重复终止幂等，未知或过期 ID 返回 `SESSION_NOT_FOUND`。

stdio 连接关闭时，服务进入生命周期清理并终止其持有的活跃会话。服务重启不会恢复旧 ID，已逃逸出受管理进程组的进程不保证被追踪。

### 常用错误处理

`INVALID_ARGUMENT` 表示参数缺失、类型/长度错误或额外字段。`INVALID_CWD` 表示目录无效，`CWD_NOT_ALLOWED` 表示越界，`ENV_NOT_ALLOWED` 表示 env 键未批准。`COMMAND_NOT_FOUND` 表示找不到可执行文件或没有执行权限，真实 exec 失败为 `SPAWN_FAILED`，启动超时为 `SPAWN_TIMEOUT`。

未知或过期会话返回 `SESSION_NOT_FOUND`，退出后写入返回 `SESSION_NOT_RUNNING`。配额含活跃、待创建和仍保留的结束记录，达到上限返回 `SESSION_LIMIT_REACHED`，结束不一定立即释放配额。

`WRITE_TIMEOUT / INPUT_CLOSED` 应检查已写字节。`AUDIT_UNAVAILABLE` 应检查审计状态及副作用说明，不可自行关闭审计绕过。`UNKNOWN_TOOL / SERVER_SHUTTING_DOWN / INTERNAL_ERROR` 分别表示未知工具、服务关闭与已脱敏内部错误。失败后可用 ping 判断连接健康，不要把业务失败都当成连接中断。

## 4. PTY 回显与 Enter

PTY 通常回显输入，Python REPL 还输出 `>>>`；stdout/stderr 在同一 PTY 合并，不提供独立通道。下面命令文本是回显，独立一行的 `200` 才是计算输出：

```text
>>> x = 100
>>> print(x * 2)
200
>>>
```

JSON 中的 `\r\n` 是终端换行的编码显示，不表示失败。自动验收匹配独立输出行，不仅搜索子串。

真正的 JSON 编辑器解析 `"input":"x = 100\n"` 时会得到 LF；普通输入框中手打反斜杠和字母 n 则发送字面量，不会触发 Enter。**Inspector 表单建议 input 只写命令，append_newline 设为 true。** 不要求必须使用该参数，直接发送真实换行也能执行。

若 REPL 已积累错误输入，先终止再新建，避免下一条与未提交的上一条连成一行。

## 5. 生命周期、配置与安全边界

默认窗口为 **1048576 个 Unicode 字符**，不是 1 MiB 字节；默认最多 **32 个会话记录**，包含待创建、活跃及仍保留的结束记录。

结束后元数据和窗口默认保留 **600 秒**，从退出时开始计时。读取/查询不延长 TTL，读取不消费输出。后台每 1 秒清理到期记录，工具查找也检查到期；之后所有会话工具均返回 `SESSION_NOT_FOUND`。

默认启动超时 5 秒、写入超时 2 秒、终止宽限 1 秒、尾部采集期限 1 秒。数值可用启动配置调节，必须满足类型和正数校验，不接受 0、布尔值、NaN 等绕过有界限制。

### 安全配置

[`server-config.json`](server-config.json) 是用户控制的可信配置：

```json
{
  "allowed_cwd_roots": ["."],
  "allowed_env_keys": [],
  "audit_log_path": "logs/sessions.jsonl",
  "audit_max_bytes": 10485760,
  "audit_backup_count": 3,
  "audit_queue_size": 2048,
  "audit_timeout": 2
}
```

相对路径以**配置文件所在目录**为基准。配置必须属于服务运行用户，不能允许组或其他用户修改。修改后重启；MCP 工具不提供扩权入口。

会话不复制 `os.environ`，连父 PATH、HOME、USER 和 token 都不继承。仅提供固定 PATH（服务解释器所在目录加 `os.defpath`）、TERM=`xterm-256color`、LANG=`C.UTF-8`。程序运行后可以自行修改环境，限制的是服务传入的初始基线。

`allowed_env_keys=[]` 默认拒绝所有新增/覆盖键，包括 PATH。需要 APP_MODE 时由用户批准 `"allowed_env_keys":["APP_MODE"]` 并重启，再显式传 `env={"APP_MODE":"dev"}`。批准键名不自动继承父值，Agent 不应自行扩大白名单。

cwd 先 `resolve(strict=True)` 再检查目录关系，拒绝外部目录、`..` 越界、同前缀兄弟目录及指向外部的符号链接。空根列表是配置错误，不表示全放开。

### OS 身份与 JSONL 审计

使用 `pwd.getpwuid(os.geteuid())` 和有效 GID 获取实际身份，PTY 继承服务 OS 凭证，不用 env.USER 判断用户，不提供切换用户参数。应以期望的普通帐号启动，sudo 会改变实际身份，服务不自动降权。

JSONL 每行完整 JSON，包含 schema_version、实例 ID、事件序号/时间，以及相关会话 ID、创建时间、实际用户、命令/argv、cwd、PID、状态、退出码和时长。记录创建/拒绝/取消、输入请求/写入/失败/取消、终止、退出、过期和关闭；读输出/查状态不逐次落盘。

不记录 input 文本、output 内容或 env 值，输入只记字节数和换行标记。但 command/argv 按需求记录，**不要把密钥直接写进命令**，无法保证任意源码自动脱敏。

新日志目录 0700，日志/锁/备份 0600；不安全的既有路径拒绝使用。后台线程、有界队列、跨进程 `flock` 负责写入与轮转；关键事件异步等待落盘 ack。默认约 10 MiB 阈值轮转，保留 3 个备份，日志历史独立于会话内存 TTL。

审计不可用时，创建和输入在提交前拒绝；若启动后才失败则回收未交付进程，但可能已有副作用，详情提供 `execution_started`。写入后才失败由 `bytes_written` 提示已提交内容。故障不会阻止终止/退出回收，audit_error/stderr 可观察；修复存储或权限后需要重启。

这些措施**不是 OS 沙箱**：cwd 白名单只限制启动目录，不限制之后 chdir/open、网络访问或命令自身 setuid；同 UID 命令仍可访问其有权限的文件。当前不提供命令级危险操作审批、重启恢复、跨客户端授权隔离、逃逸进程追踪或不可篡改审计，强杀/异常存储也无法保证最后日志完整。

## 6. Inspector 人工验证

需要 Node.js/npm，首次 npx 可能下载 Inspector。在项目根准备客户端配置，例如保存为 `mcp-client.json`：

```json
{
  "mcpServers": {
    "mcp-terminal": {
      "command": ".venv/bin/python",
      "args": ["-m", "mcp_terminal", "--config", "server-config.json"]
    }
  }
}
```

启动 Inspector：

```bash
npx --yes @modelcontextprotocol/inspector@2.12.0 \
  --config mcp-client.json --server mcp-terminal
```

相对路径示例要求客户端从项目根启动；其他启动上下文需配置对应工作目录或部署路径。手动填写时，command 为解释器，args 是四个独立参数，不带多余空格，不把 `-m mcp_terminal` 当作一项。

打开 Inspector 界面，**保持同一连接**：

1. `ping({})` 返回 pong；`start_session({"command":"python3 -i -q"})` 后保存 ID。
2. `write_input({"session_id":"ID","input":"x = 100","append_newline":true})`。
3. `write_input({"session_id":"ID","input":"print(x * 2)","append_newline":true})`。
4. `read_output({"session_id":"ID","cursor":0})`；未见独立行 200 时，用 next_cursor 继续轮询。
5. 查 get_status，应为 running；调用 terminate_session，确认 terminated、整数 exit_code、output_complete=true。

更新服务后断开并重新连接，刷新进程和 schema。多次独立 Inspector CLI 调用通常启动多个服务进程，**不能当成同一有状态会话**；连续流程用界面或 SDK 客户端。

## 7. 自动验收：统一脚本

[`outputs/validation/run_verification.py`](outputs/validation/run_verification.py) 默认先跑真实 stdio E2E，再跑全部测试，分别保存结果：

```bash
.venv/bin/python outputs/validation/run_verification.py
```

只跑 E2E、重复三轮、仅跑全量或查看参数：

```bash
.venv/bin/python outputs/validation/run_verification.py --suite e2e
.venv/bin/python outputs/validation/run_verification.py --suite e2e --repeat 3
.venv/bin/python outputs/validation/run_verification.py --suite full
.venv/bin/python outputs/validation/run_verification.py --help
```

脚本按自身位置定位项目，用项目虚拟环境启动 pytest，无需激活 venv。失败返回非零退出码，不把缺失结果、跳过或失败伪装成通过。默认单阶段超时 180 秒，可用 `--timeout` 调整。正常测试用上下文/finally 清理；硬超时仅有限停止本轮 pytest 进程组，不能保证所有已脱离组的 PTY 子进程都被强行回收。

`summary.json` 和各阶段 JUnit XML 写入 `outputs/validation/results/<运行ID>/`，控制台日志写入 `.work/validation/<运行ID>/`，脚本最后打印结果位置。both 的 full 阶段会再次执行 E2E，统计分别列出，不能相加当作唯一测试数。

### E2E 覆盖

E2E 经独立服务进程的真实 stdio JSON-RPC，不直接调用 Manager：

- `test_acceptance_e2e.py`：持续输出、Unicode 跨页、无重叠 cursor、0/非零退出、多会话隔离、24 字符窗口淘汰/恢复、输入错误后连接可用、完整 REPL、600 秒保留约定、关闭连接回收活跃进程。
- `test_security_e2e.py`：批准/拒绝 env、cwd 越界、错误不泄值、正常/并发审计、终态 duration 固定。
- `test_lifecycle_e2e.py`：短 TTL 删除元数据/输出、持续读取不延期、配额和终止等待时另一会话仍可用、真实 exec 失败/信号退出、真实审计权限故障下拒绝创建/写入但仍能终止。
- 正式入口专项：真实 stdio 链路、正式 --config 启动、实际 UID/GID 和独立 REPL 客户端子进程。

**全量测试不等于全部 E2E。** full 还包含 ring buffer 单元、PTY 底层集成、进程内 MCP、PTY/管道/I/O 故障注入、取消/超时、日志队列/ack/轮转等测试。它们覆盖底层分支，但不应标成独立 stdio 端到端。E2E 覆盖已定义的核心业务验收，不宣称穷尽所有操作系统故障。

### 验证结果

版本 0.3.0 的最新统一验收：**17 个真实 stdio E2E 通过，137 个全量测试通过**，两个阶段均无 failure、error 或 skipped。Ruff 检查与格式检查通过。后续增加测试时以 summary.json 和 JUnit 的实际计数为准。

### 独立 REPL 客户端与直接 pytest

[`outputs/stage2/repl_client.py`](outputs/stage2/repl_client.py) 只演示核心 REPL，**不是全场景脚本**。它在同一连接里设置变量、验证独立行 200、按 cursor 读取、查状态并终止：

```bash
.venv/bin/python outputs/stage2/repl_client.py
```

也可直接运行测试和静态检查：

```bash
.venv/bin/python -m pytest tests/test_acceptance_e2e.py -v -W error
.venv/bin/python -m pytest tests/test_security_e2e.py tests/test_lifecycle_e2e.py -v -W error
.venv/bin/python -m pytest -q -W error
.venv/bin/ruff check .
.venv/bin/ruff format --check .
```

小缓冲和短 TTL 用测试启动器注入配置，仍运行生产 create_server，不修改正式默认值，不真的等待十分钟。测试只操作本轮进程与临时文件，终止或关闭连接时清理。
