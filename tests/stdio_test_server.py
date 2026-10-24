"""测试启动器：通过 JSON 配置启动生产 create_server，不替换任何工具或会话实现。"""

import json
import sys

from mcp_terminal.server import create_server
from mcp_terminal.sessions import SessionConfig

if __name__ == "__main__":
    create_server(SessionConfig(**json.loads(sys.argv[1]))).run(transport="stdio")
