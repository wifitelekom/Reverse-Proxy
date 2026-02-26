#!/bin/bash
# P2P Gossip Load Balancer - Cluster Yönetim Scripti
# Kullanım: ./scripts/run_cluster.sh [start|stop|restart|status|build]

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$PROJECT_ROOT/bin"
PID_DIR="/tmp/p2p-cluster"
LOG_DIR="/tmp/p2p-cluster/logs"

build() {
    echo "=== Derleniyor ==="
    mkdir -p "$BIN_DIR"
    cd "$PROJECT_ROOT"

    go build -o "$BIN_DIR/node" ./cmd/node/
    echo "✓ node derlendi"

    go build -o "$BIN_DIR/loadbalancer" ./cmd/loadbalancer/
    echo "✓ loadbalancer derlendi"

    go build -o "$BIN_DIR/simulator" ./cmd/simulator/
    echo "✓ simulator derlendi"

    echo "Derleme tamamlandı."
}

start_cluster() {
    mkdir -p "$PID_DIR" "$LOG_DIR"

    if [ ! -f "$BIN_DIR/node" ]; then
        echo "Binary bulunamadı. Önce 'build' çalıştırın."
        exit 1
    fi

    echo "=== Cluster başlatılıyor ==="

    # Node A — seed olarak B'yi ve C'yi gösterir
    "$BIN_DIR/node" \
        -id=node-a \
        -gossip=:9001 \
        -service=:8001 \
        -seeds=:9002,:9003 \
        > "$LOG_DIR/node-a.log" 2>&1 &
    echo $! > "$PID_DIR/node-a.pid"
    echo "✓ node-a başlatıldı (HTTP :8001, Gossip UDP :9001)"

    # Node B
    "$BIN_DIR/node" \
        -id=node-b \
        -gossip=:9002 \
        -service=:8002 \
        -seeds=:9001,:9003 \
        > "$LOG_DIR/node-b.log" 2>&1 &
    echo $! > "$PID_DIR/node-b.pid"
    echo "✓ node-b başlatıldı (HTTP :8002, Gossip UDP :9002)"

    # Node C
    "$BIN_DIR/node" \
        -id=node-c \
        -gossip=:9003 \
        -service=:8003 \
        -seeds=:9001,:9002 \
        > "$LOG_DIR/node-c.log" 2>&1 &
    echo $! > "$PID_DIR/node-c.pid"
    echo "✓ node-c başlatıldı (HTTP :8003, Gossip UDP :9003)"

    # Node D
    "$BIN_DIR/node" \
        -id=node-d \
        -gossip=:9004 \
        -service=:8004 \
        -seeds=:9001,:9002 \
        > "$LOG_DIR/node-d.log" 2>&1 &
    echo $! > "$PID_DIR/node-d.pid"
    echo "✓ node-d başlatıldı (HTTP :8004, Gossip UDP :9004)"

    # Node'ların birbirini keşfetmesi için bekle
    echo "Node'lar gossip ile birbirini keşfediyor (1s)..."
    sleep 1

    # Load Balancer — tüm node'ları seed olarak gösterir
    "$BIN_DIR/loadbalancer" \
        -listen=:8080 \
        -strategy=roundrobin \
        -gossip=:9000 \
        -seeds=:9001,:9002,:9003,:9004 \
        > "$LOG_DIR/lb.log" 2>&1 &
    echo $! > "$PID_DIR/lb.pid"
    echo "✓ loadbalancer başlatıldı (HTTP :8080, Gossip UDP :9000)"

    echo ""
    echo "=== Cluster hazır ==="
    echo "Test: curl http://localhost:8080/"
    echo "Gossip state: curl http://localhost:8001/members"
    echo "Logs: $LOG_DIR/"
}

stop_cluster() {
    echo "=== Cluster durduruluyor ==="

    for pid_file in "$PID_DIR"/*.pid; do
        if [ -f "$pid_file" ]; then
            name=$(basename "$pid_file" .pid)
            pid=$(cat "$pid_file")
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid"
                echo "✓ $name durduruldu (PID: $pid)"
            else
                echo "  $name zaten durmuş"
            fi
            rm -f "$pid_file"
        fi
    done

    echo "Cluster durduruldu."
}

status_cluster() {
    echo "=== Cluster Durumu ==="

    for pid_file in "$PID_DIR"/*.pid; do
        if [ -f "$pid_file" ]; then
            name=$(basename "$pid_file" .pid)
            pid=$(cat "$pid_file")
            if kill -0 "$pid" 2>/dev/null; then
                echo "✓ $name ÇALIŞIYOR (PID: $pid)"
            else
                echo "✗ $name DURMUŞ"
            fi
        fi
    done

    echo ""
    echo "=== HTTP Health Check ==="
    for port in 8001 8002 8003 8004; do
        if curl -sf "http://localhost:$port/health" > /dev/null 2>&1; then
            echo "✓ :$port SAĞLIKLI"
        else
            echo "✗ :$port ERİŞİLEMİYOR"
        fi
    done

    if curl -sf "http://localhost:8080/" > /dev/null 2>&1; then
        echo "✓ :8080 LB ÇALIŞIYOR"
    else
        echo "✗ :8080 LB ERİŞİLEMİYOR"
    fi
}

case "${1:-help}" in
    build)
        build
        ;;
    start)
        start_cluster
        ;;
    stop)
        stop_cluster
        ;;
    restart)
        stop_cluster
        sleep 1
        start_cluster
        ;;
    status)
        status_cluster
        ;;
    *)
        echo "Kullanım: $0 {build|start|stop|restart|status}"
        echo ""
        echo "Komutlar:"
        echo "  build    - Tüm binary'leri derle"
        echo "  start    - Cluster başlat (4 node + LB)"
        echo "  stop     - Cluster durdur"
        echo "  restart  - Cluster yeniden başlat"
        echo "  status   - Cluster durumunu göster"
        exit 1
        ;;
esac
