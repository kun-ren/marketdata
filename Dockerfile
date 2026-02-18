# 1. 使用轻量级的 Python 基础镜像
FROM python:3.10-slim

# 2. 设置工作目录（容器内部的路径）
WORKDIR /app

# 3. 先复制依赖文件（利用 Docker 缓存机制）
# 只要 requirements.txt 没变，下次构建会直接跳过 pip install
COPY . .

# 4. 安装依赖
# --no-cache-dir 可以减小镜像体积
RUN pip install --no-cache-dir -r requirements.txt



# 6. 设置环境变量（可选，防止 python 产生 pyc 文件及启用日志实时刷新）
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 6. 创建宿主机挂载点目录（容器内路径）
RUN mkdir -p /app/dataset

# 7. 设置 /app/dataset 为卷（挂载点）
VOLUME ["/app/dataset"]

# 7. 指定启动命令
CMD ["python", "-m", "src.downloader"]