# 从零手写 Go 智能体平台

本项目是一个用 Go 实现的**通用智能体（Agent）平台**，提供流式对话、知识库检索、多智能体协作和工作流编排。

- **ReAct 智能体**支持 Function Calling。运行时根据模型输出执行工具并回传结果，通过 SSE 在控制台展示回复、工具调用和模型返回的推理内容。
- **RAG 知识库**支持文档上传、解析、分块和向量化。检索结合 Milvus 稠密向量与 PostgreSQL 关键词检索，用 RRF 融合排名，并支持可选的模型重排和检索测试台。
- **多智能体协作**采用 Host/Expert 模式。Host 负责路由和聚合，Expert 负责专项执行，A2A 用于委派任务给远程 Agent。
- **工作流引擎**使用 YAML DSL 编排节点和条件分支，支持在 HITL 审批节点保存执行状态，并在审批后继续执行。
- **MCP 集成**提供 MCP Client，可注册外部服务、同步和调试工具，并检查服务健康状态。
- **长期记忆**定期从对话中提取记忆，并在后续会话中检索相关内容。
- **生产基建**包括 JWT 双 Token、API Key 的 AES-GCM 加密、OpenTelemetry 链路追踪、Prometheus 指标和基于 Outbox 与 Kafka 的文档索引任务。

可以先读极简循环，再对照服务端实现中的模型调用、工具执行和流式事件。[手写 ReAct 循环](backend/internal/agent/naive/react_loop.go) · [服务端 Runtime](backend/internal/agent/runtime/runner.go)


## 技术栈

| 层 | 技术 |
| --- | --- |
| 后端 | Go 1.26 · Hertz · GORM · PostgreSQL 16 · Redis 7 · Kafka 4.1 · Milvus 2.5 · MinIO · golang-migrate |
| AI / Agent | DeepSeek / Ollama（OpenAI 兼容协议）· Go Agent Runtime · MCP Client · Docker 代码沙箱 · SearXNG |
| 前端 | React 18.3 · TypeScript 5.6 · Tailwind CSS 3.4 · GSAP 3.13 · Zustand 5 · React Router 6 · Vite 5 |
| 可观测性 | OpenTelemetry · Jaeger · Prometheus · Grafana · zap |

## 快速开始

### 环境要求

- Go 1.26 或以上版本
- Node.js 18 或以上版本，以及 npm
- Docker 和 Docker Compose
- 使用 DeepSeek 在线对话时需准备 API Key

以下命令以 PowerShell 为例，从仓库根目录开始执行。

### ① 启动基础设施

```powershell
cd backend
docker compose -f deployments/docker-compose.yaml --profile full up -d
docker compose -f deployments/docker-compose.yaml --profile full ps
```

`full` 会启动 PostgreSQL、Redis、MinIO、Kafka、Milvus、Ollama 和 SearXNG。后端默认在启动时就连接 Milvus，因此运行 API 时需要一个可达的 Milvus 实例。

需要 Jaeger、Prometheus 和 Grafana 时，在启动命令中同时启用 `--profile full --profile obs`。

### ② 执行数据库迁移并初始化模型

等 PostgreSQL 显示 `healthy` 后，在 `backend/` 下执行迁移。有 GNU Make 时也可运行 `make migrate`。

```powershell
go run ./cmd/migrate
```

迁移会预置模型供应商，并在用户表为空时创建本地管理员 `admin / admin123`。首次使用空数据库时，还要为启动检查补齐 `deepseek-chat` 和 `bge-m3` 两条模型配置。以下示例使用 DeepSeek 对话和 Ollama 向量模型，保留已有的同别名启用配置。

```powershell
@'
WITH defaults(provider_code, model_name, alias, type) AS (
    VALUES
        ('deepseek', 'deepseek-chat', 'deepseek-chat', 'chat'),
        ('ollama',   'bge-m3',        'bge-m3',        'embedding')
)
INSERT INTO model_configs (provider_id, model_name, alias, type, is_default)
SELECT p.id, d.model_name, d.alias, d.type, TRUE
FROM defaults AS d
JOIN model_providers AS p ON p.code = d.provider_code
WHERE NOT EXISTS (
    SELECT 1 FROM model_configs AS m
    WHERE m.alias = d.alias AND m.type = d.type AND m.status = 1
);
'@ | docker compose -f deployments/docker-compose.yaml --profile full exec -T postgres psql -v ON_ERROR_STOP=1 -U nexus -d nexus_agent
```

### ③ 启动后端 API

在 `backend/` 下执行以下命令，或运行 `make run`。

```powershell
go run ./cmd/server
```

后端 API 默认监听 `8080`，A2A 默认监听 `8082`。可访问 [健康检查](http://localhost:8080/healthz)确认 API 已启动。

### ④ 启动前端控制台

另开一个终端，从仓库根目录执行以下命令。

```powershell
cd frontend
npm ci
npm run dev
```

前端默认监听 `5173`，Vite 会将 `/api` 和 `/healthz` 转发到后端。

### ⑤ 配置模型并开始对话

1. 用 `admin / admin123` 登录。[前端控制台](http://localhost:5173)
2. 在 **模型管理 → 模型供应商** 中填写 DeepSeek 的 API Key。如需更换模型，在同页的 **模型配置** 中填写供应商实际提供的模型名。
3. 回到 **对话页**，发送一条消息查看流式回复。模型调用工具时会显示决策链，模型返回推理内容时会显示对应的推理文本。

如果使用 Ollama 对话，先下载所选对话模型，再在 **模型管理** 中添加 Ollama 的 `chat` 配置。将 `llm.default_chat` 指向该配置的别名，并重启后端。Ollama 供应商默认地址为 `http://127.0.0.1:11434/v1`。[后端配置](backend/configs/config.yaml)

### ⑥ 体验 RAG 知识库

如果沿用示例中的 Ollama 向量模型，在新终端进入 `backend/`，下载 `bge-m3` 后启动 Worker。

```powershell
docker compose -f deployments/docker-compose.yaml --profile full exec ollama ollama pull bge-m3
go run ./cmd/worker
```

Worker 也可以用 `make run-worker` 启动。配置其他向量模型时，向量输出维度须与 `milvus.dim` 一致。

在控制台创建知识库并上传文档，等索引完成后，在知识库详情页的检索测试台输入问题查看召回片段。API 将原件写入 MinIO，并在 PostgreSQL 登记待发任务。Worker 将任务发布到 Kafka，随后消费任务并构建索引。Compose 会创建 `doc-etl` 和 `doc-etl.dlq`，连接地址、主题和消费组可在 `kafka` 部分调整。[后端配置](backend/configs/config.yaml)

从 Redis Streams 版本升级时，先停止旧版 API 和 Worker，执行迁移后再启动新版。迁移会登记未完成的文档，Worker 核对 MinIO 原件是否存在且大小一致后会补投。

## 项目结构

```text
Nexus-Agent/
├── README.md
├── docs/                     # 项目配图
├── backend/
│   ├── cmd/
│   │   ├── server/           # API 与 A2A 服务入口
│   │   ├── worker/           # Kafka 文档索引 Worker
│   │   ├── cli/              # 终端对话入口
│   │   └── migrate/          # 数据库迁移与管理员初始化
│   ├── configs/config.yaml   # 默认配置，支持 NX_ 环境变量覆盖
│   ├── deployments/          # Docker Compose、Dockerfile 与观测栈配置
│   ├── migrations/           # 版本化 SQL 迁移
│   └── internal/
│       ├── api/               # HTTP 路由与处理器（REST + SSE）
│       ├── service/           # 业务编排
│       ├── model/             # 数据模型
│       ├── repo/              # PostgreSQL 数据访问
│       ├── agent/             # 手写 ReAct、Runtime 与 Host 路由
│       ├── rag/               # 文档索引、混合检索与 RRF
│       ├── vector/            # 向量存储接口与 Milvus 实现
│       ├── workflow/          # YAML DSL 与 HITL 审批
│       ├── memory/            # 长期记忆抽取与检索
│       ├── tool/              # 工具注册、执行与沙箱
│       ├── llm/               # 供应商适配、重试、熔断与流式请求
│       ├── mcp/               # MCP 客户端
│       ├── a2a/               # 远程 Agent 客户端与服务端
│       ├── infra/             # PostgreSQL、Redis、Milvus 与 MinIO 连接
│       ├── security/          # JWT 与 AES-GCM 加密
│       ├── observability/     # 链路追踪与指标
│       └── worker/            # Outbox 发布与 Kafka 消费
└── frontend/
    └── src/
        ├── api/               # 类型化接口封装
        ├── pages/             # 登录、对话、知识库、工作流与管理页面
        ├── stores/            # Zustand 状态与 SSE 事件处理
        ├── lib/               # 请求、Token 刷新与 SSE 解析
        ├── components/        # 布局与对话组件
        └── types/             # 前端数据类型
```

## 常用端口速查

| 服务 | 默认地址 | 说明 |
| --- | --- | --- |
| 前端控制台 | [localhost:5173](http://localhost:5173) | 本地管理员 `admin / admin123` |
| 后端 API | [localhost:8080](http://localhost:8080/healthz) | REST、SSE 与健康检查 |
| A2A | [localhost:8082](http://localhost:8082/.well-known/agent.json) | Agent 名片与远程调用 |
| PostgreSQL | `127.0.0.1:5432` | 业务数据、文档元数据与 Outbox |
| Redis | `127.0.0.1:6380` | 宿主机端口映射到容器内的 `6379` |
| Kafka | `127.0.0.1:9092` | 文档索引任务与死信队列 |
| MinIO | `127.0.0.1:9000` · [localhost:9001](http://localhost:9001) | 对象存储 API 与管理控制台 |
| Milvus | `127.0.0.1:19530` | 向量检索 gRPC 端口 |
| Ollama | [localhost:11434](http://localhost:11434) | 本地模型服务 |
| SearXNG | [localhost:8081](http://localhost:8081) | 联网搜索 |
| Jaeger | [localhost:16686](http://localhost:16686) | 链路追踪，需启用 `obs` |
| Prometheus | [localhost:9090](http://localhost:9090) | 指标查询，需启用 `obs` |
| Grafana | [localhost:3004](http://localhost:3004) | 监控面板，需启用 `obs` |

## 常见问题

- **迁移提示 PostgreSQL 连接失败。** 确认 Docker 已运行、PostgreSQL 容器显示 `healthy`，以及宿主机端口 `5432` 可用。
- **启动时报 `Milvus Proxy is not ready yet`。** 确认已启用 `full`，等 Milvus 初始化后重试。可用 `docker compose -f deployments/docker-compose.yaml --profile full logs --tail 100 milvus` 查看进度。
- **启动时报默认模型未配置。** 执行第 ② 步的 SQL，并检查启用的模型别名是否与 `llm.default_chat` 和 `llm.default_embedding` 对应。
- **对话报模型错误。** 检查供应商地址、API Key 和上游模型名。使用 Ollama 时，确认所选模型已下载。
- **上传后一直等待索引。** 确认 Worker 已运行、Kafka 的初始化容器已成功退出，并检查向量模型是否可用。
- **本机端口冲突。** 调整 Compose 的宿主机端口并同步后端连接地址。修改后端 HTTP 端口时，把前端的 `NX_BACKEND_URL` 指向新地址。[Compose 配置](backend/deployments/docker-compose.yaml) · [后端配置](backend/configs/config.yaml)
