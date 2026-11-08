#!/usr/bin/env bash
# =============================================================
# 纯内网离线部署：在「有网机器」打包镜像 → 拷进内网 → 内网机器导入
#
#   有网机器（与内网同架构，通常都是 linux/amd64）：
#       bash offline.sh save            # 产出 offline/cdc-images.tar.gz
#
#   内网 B 服：
#       bash offline.sh load            # 导入镜像
#       bash deploy.sh                  # 正常部署（不会再联网拉镜像）
#
# 注意：
#   - 若打包机是 arm64（如 M 系列 Mac），务必加 --platform linux/amd64：
#       PLATFORM=linux/amd64 bash offline.sh save
#   - connect 镜像是本地构建的，save 会把 cdc-connect:latest / cdc-monitor:latest
#     一起打进去，内网无需再 build。
# =============================================================
set -euo pipefail
cd "$(dirname "$0")"

OUT_DIR="offline"
ARCHIVE="${OUT_DIR}/cdc-images.tar.gz"
PLATFORM="${PLATFORM:-}"

# 需要打包的镜像：基础镜像 + 本项目自构建镜像
BASE_IMAGES=(
  "confluentinc/cp-kafka:7.6.1"
  "confluentinc/cp-schema-registry:7.6.1"
  "confluentinc/cp-kafka-connect:7.6.1"
  "nginx:1.27-alpine"
)
BUILT_IMAGES=(
  "cdc-connect:latest"
  "cdc-monitor:latest"
)

usage() {
  echo "用法: bash offline.sh {save|load}"
  echo "  save  在【有网机器】打包全部镜像到 ${ARCHIVE}"
  echo "  load  在【内网机器】从 ${ARCHIVE} 导入镜像"
  echo ""
  echo "可选环境变量:"
  echo "  PLATFORM=linux/amd64   # 跨架构打包时指定"
  echo "  ARCHIVE=/path/x.tar.gz # 自定义归档路径"
  exit 1
}

cmd_save() {
  echo "▶ [1/3] 拉取基础镜像..."
  for img in "${BASE_IMAGES[@]}"; do
    echo "   pull $img"
    if [ -n "$PLATFORM" ]; then
      docker pull --platform "$PLATFORM" "$img"
    else
      docker pull "$img"
    fi
  done

  echo "▶ [2/3] 构建本项目镜像（connect / monitor）..."
  # 构建时必须能联网装依赖（pip / confluent-hub），所以只在有网机器做
  docker compose build connect cdc-monitor

  echo "▶ [3/3] 导出镜像包（较大，约 2~4G，耗时数分钟）..."
  mkdir -p "$(dirname "$ARCHIVE")"
  docker save "${BASE_IMAGES[@]}" "${BUILT_IMAGES[@]}" | gzip -1 > "$ARCHIVE"
  echo ""
  echo "✔ 已生成：$(pwd)/${ARCHIVE}"
  echo "  把整个 offline/ 目录拷到内网 B 服的 docker/ 下，然后执行： bash offline.sh load"
}

cmd_load() {
  [ -f "$ARCHIVE" ] || { echo "✗ 找不到镜像包：$ARCHIVE"; exit 1; }
  echo "▶ 导入镜像（约 2~4G，耗时数分钟）..."
  gunzip -c "$ARCHIVE" | docker load
  echo ""
  echo "✔ 镜像导入完成："
  docker images --format '  {{.Repository}}:{{.Tag}}  {{.Size}}' | grep -E 'cp-kafka|cp-schema|nginx|cdc-' || true
  echo ""
  echo "  下一步：确认 .env 与 docker/certs/ 已就绪，然后执行  bash deploy.sh"
}

case "${1:-}" in
  save) cmd_save ;;
  load) cmd_load ;;
  *) usage ;;
esac
