"""第二阶段：可并发控制本地 PTY 会话的 stdio MCP Server。"""

from __future__ import annotations

import argparse
import logging
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from typing import Annotated, Any

import anyio
from mcp.server import MCPServer
from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from mcp.server.mcpserver import Context
from mcp.shared.exceptions import MCPError
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from mcp_terminal.config import load_config
from mcp_terminal.errors import SessionError
from mcp_terminal.sessions import SessionConfig, SessionManager

logger = logging.getLogger(__name__)


class ErrorInfo(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class ToolResponse(BaseModel):
    """所有工具使用同一结果结构；cursor 恢复信息位于 error.details。"""

    ok: bool
    data: dict[str, Any] | None
    error: ErrorInfo | None


Result = Annotated[CallToolResult, ToolResponse]
SessionId = Annotated[
    str, Field(min_length=1, max_length=128, description="start_session 返回的 ID")
]
Command = Annotated[
    str, Field(min_length=1, max_length=8192, description="命令及参数；不隐式 shell")
]
Cursor = Annotated[int, Field(ge=0, description="Unicode 字符绝对偏移；使用上次 next_cursor")]
PageSize = Annotated[int, Field(ge=1, le=65536, description="单页最大 Unicode 字符数")]
InputText = Annotated[
    str,
    Field(min_length=1, description="原样文本，不解码反斜杠转义；可用 append_newline 发送 Enter"),
]
AppendNewline = Annotated[
    bool, Field(description="在原样输入末尾追加一个真正的 LF 换行（Enter），默认 false")
]


class _Args(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class _StartArgs(_Args):
    command: Command
    cwd: str | None = None
    env: dict[str, str] | None = None


class _SessionArgs(_Args):
    session_id: SessionId


class _WriteArgs(_SessionArgs):
    input: InputText
    append_newline: AppendNewline = False


class _ReadArgs(_SessionArgs):
    cursor: Cursor | None = None
    max_chars: PageSize = 16384


ARG_MODELS = {
    "ping": _Args,
    "start_session": _StartArgs,
    "write_input": _WriteArgs,
    "read_output": _ReadArgs,
    "get_status": _SessionArgs,
    "terminate_session": _SessionArgs,
}


def success(data: dict[str, Any]) -> CallToolResult:
    return _result(ToolResponse(ok=True, data=data, error=None))


def failure(code: str, message: str, details: dict[str, Any] | None = None) -> CallToolResult:
    return _result(
        ToolResponse(
            ok=False, data=None, error=ErrorInfo(code=code, message=message, details=details or {})
        )
    )


def _result(response: ToolResponse) -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text=response.model_dump_json())],
        structured_content=response.model_dump(mode="json"),
        is_error=not response.ok,
    )


async def _run(operation: Awaitable[dict[str, Any]]) -> CallToolResult:
    try:
        return success(await operation)
    except SessionError as exc:
        return failure(exc.code, exc.message, exc.details)
    except Exception as exc:
        logger.error("Unexpected session failure: %s", type(exc).__name__)
        return failure("INTERNAL_ERROR", "内部会话操作失败。")


async def _tool_boundary(ctx: ServerRequestContext[Any], call_next: CallNext) -> HandlerResult:
    if ctx.method != "tools/call":
        return await call_next(ctx)
    params = ctx.params
    if not isinstance(params, dict):
        return failure("INVALID_ARGUMENT", "tools/call 参数必须是对象。")
    name = params.get("name")
    if not isinstance(name, str):
        return failure("INVALID_ARGUMENT", "工具名称必须是字符串。")
    model = ARG_MODELS.get(name)
    if model is None:
        return failure("UNKNOWN_TOOL", "未知工具。")
    arguments = params.get("arguments")
    if arguments is None:
        arguments = {}
    try:
        model.model_validate(arguments)
    except ValidationError as exc:
        # 不使用包含输入值的默认 ValidationError 文本。
        fields = [
            {"field": ".".join(str(part) for part in err["loc"]), "type": err["type"]}
            for err in exc.errors(include_input=False, include_url=False)
        ]
        return failure("INVALID_ARGUMENT", "工具参数不符合接口规范。", {"fields": fields})
    try:
        result = await call_next(ctx)
    except (ValidationError, MCPError):
        return failure("INVALID_ARGUMENT", "MCP 工具调用参数无效。")
    except Exception as exc:
        logger.error("Unexpected MCP tool failure: %s", type(exc).__name__)
        return failure("INTERNAL_ERROR", "内部工具调用失败。")
    if isinstance(result, CallToolResult) and result.is_error and result.structured_content is None:
        return failure("INTERNAL_ERROR", "内部工具调用失败。")
    return result


def create_server(config: SessionConfig | None = None) -> MCPServer:
    @asynccontextmanager
    async def lifespan(_: MCPServer) -> AsyncIterator[SessionManager]:
        manager = SessionManager(config)
        await manager.__aenter__()
        try:
            yield manager
        finally:
            # stdio 断开 / AnyIO 取消时，清理也必须能继续 await。
            with anyio.CancelScope(shield=True):
                await manager.close()

    app = MCPServer(
        "mcp-terminal",
        version="0.3.0",
        lifespan=lifespan,
        middleware=[_tool_boundary],
        instructions=(
            "本地PTY会话控制。先start_session，再write_input/read_output/get_status，最后terminate_session。"
            "所有结果为ok/data/error；失败检查error.code。read_output不等待、不消费，"
            "用data.next_cursor增量读取；CURSOR_EXPIRED时从error.details.earliest_cursor恢复。"
            "write_input成功仅表示提交，非执行完成。PTY含回显，stdout/stderr合并。"
            "cwd限于服务配置允许目录；新env键或覆盖需用户先在本地配置批准，"
            "ENV_NOT_ALLOWED时请用户配置，不得自动扩大权限。会话使用服务的OS用户身份。"
        ),
    )

    @app.tool(
        title="检查服务连通性",
        annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False),
    )
    def ping() -> Result:
        """无需参数；返回统一结构，data 中为 status=ok、message=pong。"""
        return success({"status": "ok", "message": "pong"})

    @app.tool(
        title="创建独立终端会话",
        annotations=ToolAnnotations(
            read_only_hint=False,
            destructive_hint=False,
            idempotent_hint=False,
            open_world_hint=True,
        ),
    )
    async def start_session(
        command: Command,
        ctx: Context[SessionManager],
        cwd: str | None = None,
        env: dict[str, str] | None = None,
    ) -> Result:
        """创建独立PTY，不隐式shell；cwd须位于用户允许根目录，env仅允许已批准键。

        ENV_NOT_ALLOWED时须请用户修改启动配置，不可自行扩权。子进程使用服务实际OS身份。
        """
        return await _run(ctx.request_context.lifespan_context.start_session(command, cwd, env))

    @app.tool(
        title="向会话写入交互输入",
        annotations=ToolAnnotations(
            read_only_hint=False, destructive_hint=True, idempotent_hint=False, open_world_hint=True
        ),
    )
    async def write_input(
        session_id: SessionId,
        input: InputText,
        ctx: Context[SessionManager],
        append_newline: AppendNewline = False,
    ) -> Result:
        """原样写入PTY；append_newline=true可追加真实Enter，适合Inspector输入字段。

        不会将字面量反斜杠n转换为换行。含追加换行最多65536 UTF-8字节；成功仅表示提交。
        """
        return await _run(
            ctx.request_context.lifespan_context.write_input(session_id, input, append_newline)
        )

    @app.tool(
        title="按cursor读取终端输出",
        annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False),
    )
    async def read_output(
        session_id: SessionId,
        ctx: Context[SessionManager],
        cursor: Cursor | None = None,
        max_chars: PageSize = 16384,
    ) -> Result:
        """立即读取窗口，不等待、不消费。cursor为字符偏移，默认最早位置。保存next_cursor继续读取。

        CURSOR_EXPIRED表示旧输出已覆盖；从error.details.earliest_cursor恢复，无法重取已丢弃数据。
        """
        return await _run(
            ctx.request_context.lifespan_context.read_output(session_id, cursor, max_chars)
        )

    @app.tool(
        title="查看会话状态",
        annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False),
    )
    async def get_status(session_id: SessionId, ctx: Context[SessionManager]) -> Result:
        """返回后台更新的状态、退出码、时间及输出窗口。结束记录保留600秒，查询不延期。"""
        return await _run(ctx.request_context.lifespan_context.get_status(session_id))

    @app.tool(
        title="终止会话及其进程组",
        annotations=ToolAnnotations(
            read_only_hint=False, destructive_hint=True, idempotent_hint=True, open_world_hint=False
        ),
    )
    async def terminate_session(session_id: SessionId, ctx: Context[SessionManager]) -> Result:
        """SIGTERM后有限等待，必要时SIGKILL并回收；重复调用幂等。其他会话不受等待影响。"""
        return await _run(ctx.request_context.lifespan_context.terminate_session(session_id))

    return app


mcp = create_server()


def main() -> None:
    parser = argparse.ArgumentParser(description="本地 PTY MCP Server")
    parser.add_argument("--config", help="用户控制的本地 JSON 配置；相对路径以配置目录为基准")
    args = parser.parse_args()
    if args.config:
        try:
            config = load_config(args.config)
        except (OSError, ValueError, TypeError):
            parser.error("无法加载安全配置，请检查文件权限、路径和字段类型。")
        create_server(config).run(transport="stdio")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
