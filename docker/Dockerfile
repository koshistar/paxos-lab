# 用最简的官方镜像：我们只用标准库，不需要编译任何东西
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY paxos/ /app/paxos/

# 数据卷挂载点：每个容器自己的 Acceptor/Learner 日志
RUN mkdir -p /data
VOLUME ["/data"]

# 统一入口：compose 里用 command 指定子命令，例如
#   command: ["node","--id","n1", ...]
ENTRYPOINT ["python", "-m", "paxos.cli"]
CMD ["--help"]

# 健康检查：管理口能应答就说明节点在跑
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import socket,sys; s=socket.create_connection(('127.0.0.1', int('${ADMIN_PORT:-8001}')), 2); s.close()"
