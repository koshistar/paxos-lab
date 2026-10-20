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

# iproute2 里有 tc —— 用来做"内核级网络时延/丢包"的对照实验（配合 cap_add: NET_ADMIN）。
# python:slim 镜像里没有它，不装的话 docker exec ... tc 会报 "tc: not found"。
RUN apt-get update \
    && apt-get install -y --no-install-recommends iproute2 \
    && rm -rf /var/lib/apt/lists/*

# 统一入口：compose 里用 command 指定子命令，例如
#   command: ["node","--id","n1", ...]
ENTRYPOINT ["python", "-m", "paxos.cli"]

# 健康检查：容器内的管理口固定是 8001（宿主机映射成 8101~8104）。
# 这里**不要**用环境变量：之前用 ${ADMIN_PORT} 时，n2/n3/n4 的环境变量是
# 宿主机端口（8002~8004），而容器里实际监听的永远是 8001，于是探活连错端口，
# 四个容器全是 unhealthy —— 一个很典型的"宿主机端口 vs 容器端口"混淆坑。
HEALTHCHECK --interval=10s --timeout=3s --start-period=8s --retries=3 \
    CMD python -c "import socket; socket.create_connection(('127.0.0.1', 8001), 2).close()"
