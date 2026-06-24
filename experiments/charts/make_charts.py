#!/usr/bin/env python3
"""Generate thesis-grade chart images from the experiment JSON output.

Reads the JSON produced by the programs under experiments/ and writes
PNG figures suitable for the report and the defense slides.

Usage:
    python make_charts.py <json_dir> <out_dir>

Defaults: json_dir = ".", out_dir = "./charts".

Expected input files in json_dir (missing ones are skipped with a note):
    hetero.json   <- experiments/heterogeneous_bench
    stress.json   <- experiments/stress
    mtls.json     <- experiments/mtls_bench
    soak.json     <- experiments/soak

Dependencies: matplotlib (pip install matplotlib).
"""

import json
import os
import sys

import matplotlib
matplotlib.use("Agg")  # headless: write files, no display window
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

# ---- shared style: white background, deck-matching accent palette ----------
INK = "#14322E"      # deep petrol — primary text / neutral bars
AMBER = "#C97A12"    # accent / "medium"
SAGE = "#6B7F75"     # muted neutral bars
GOOD = "#2F7D55"     # green — the winning / healthy case
BAD = "#B23A2E"      # red — killed / slow
GRID = "#D9D2C6"

plt.rcParams.update({
    "figure.dpi": 150,
    "savefig.dpi": 150,
    "font.size": 12,
    "font.family": "sans-serif",
    "axes.edgecolor": INK,
    "axes.labelcolor": INK,
    "text.color": INK,
    "xtick.color": INK,
    "ytick.color": INK,
    "axes.titlesize": 15,
    "axes.titleweight": "bold",
})


def load(json_dir, name):
    path = os.path.join(json_dir, name)
    if not os.path.exists(path):
        print(f"  skip: {name} not found in {json_dir}")
        return None
    with open(path) as f:
        return json.load(f)


def style_axes(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)


# ===========================================================================
def chart_hetero_p99(data, out):
    """Headline: tail latency (p99) per selector on the mixed cluster."""
    if not data:
        return
    runs = data["runs"]
    labels = {"roundrobin": "Round-Robin", "leastconn": "Least-Conn",
              "p2c": "P2C", "p2c-ewma": "P2C-EWMA"}
    names = [labels.get(r["selector"], r["selector"]) for r in runs]
    p99 = [r["p99_ms"] for r in runs]
    colors = [GOOD if r["selector"] == "p2c-ewma" else SAGE for r in runs]

    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    bars = ax.bar(names, p99, color=colors, zorder=3, width=0.6)
    for b, v in zip(bars, p99):
        ax.text(b.get_x() + b.get_width() / 2, v + 0.4, f"{v:.1f} ms",
                ha="center", va="bottom", fontweight="bold", fontsize=12)
    ax.set_ylabel("p99 latency (ms) — lower is better")
    ax.set_title("Tail latency by selector  ·  backends at 1 / 5 / 20 ms")
    ax.set_ylim(0, max(p99) * 1.18)
    style_axes(ax)
    # call out the win
    ax.annotate("~3× lower", xy=(3, p99[-1]), xytext=(3, max(p99) * 0.6),
                ha="center", color=GOOD, fontweight="bold", fontsize=12,
                arrowprops=dict(arrowstyle="->", color=GOOD, lw=1.5))
    fig.tight_layout()
    fig.savefig(os.path.join(out, "hetero_p99.png"))
    plt.close(fig)
    print("  wrote hetero_p99.png")


def chart_hetero_distribution(data, out):
    """Why it wins: traffic share to fast/medium/slow per selector."""
    if not data:
        return
    runs = data["runs"]
    labels = {"roundrobin": "Round-Robin", "leastconn": "Least-Conn",
              "p2c": "P2C", "p2c-ewma": "P2C-EWMA"}
    names = [labels.get(r["selector"], r["selector"]) for r in runs]
    fast = [r["per_backend"]["node-fast"]["share_pct"] for r in runs]
    med = [r["per_backend"]["node-medium"]["share_pct"] for r in runs]
    slow = [r["per_backend"]["node-slow"]["share_pct"] for r in runs]

    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    ax.barh(names, fast, color=GOOD, zorder=3, label="fast (1 ms)")
    ax.barh(names, med, left=fast, color=AMBER, zorder=3, label="medium (5 ms)")
    left2 = [f + m for f, m in zip(fast, med)]
    ax.barh(names, slow, left=left2, color=BAD, zorder=3, label="slow (20 ms)")
    for i, s in enumerate(slow):
        ax.text(100.5, i, f"{s:.1f}% to slow", va="center", fontsize=10, color=BAD)
    ax.set_xlim(0, 118)
    ax.set_xlabel("share of traffic (%)")
    ax.set_title("Where each selector sends traffic")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(loc="lower right", frameon=False, fontsize=10, ncol=3,
              bbox_to_anchor=(1.0, -0.28))
    fig.tight_layout()
    fig.savefig(os.path.join(out, "hetero_distribution.png"))
    plt.close(fig)
    print("  wrote hetero_distribution.png")


def chart_stress(data, out):
    """Per-backend hit distribution; killed node highlighted."""
    if not data:
        return
    pb = data["per_backend"]
    names = [b["id"] for b in pb]
    hits = [b["hits"] for b in pb]
    colors = [BAD if b.get("killed") else INK for b in pb]
    cv = data.get("cv_survivors_phase_b", 0) * 100

    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    bars = ax.bar(names, hits, color=colors, zorder=3, width=0.6)
    for b, v, info in zip(bars, hits, pb):
        tag = "killed" if info.get("killed") else ""
        ax.text(b.get_x() + b.get_width() / 2, v + 3, f"{v}\n{tag}".strip(),
                ha="center", va="bottom", fontsize=10,
                color=BAD if info.get("killed") else INK)
    # mean line over survivors
    surv = [b["hits"] for b in pb if not b.get("killed")]
    mean = sum(surv) / len(surv)
    ax.axhline(mean, color=SAGE, linestyle="--", linewidth=1.2, zorder=2)
    ax.text(len(names) - 0.5, mean + 4, f"survivor mean {mean:.0f}",
            ha="right", color=SAGE, fontsize=9)
    ax.set_ylabel("requests served")
    ax.set_title(f"Distribution under failure  ·  CV {cv:.1f}%  (target < 15%)")
    ax.set_ylim(0, max(hits) * 1.2)
    style_axes(ax)
    fig.tight_layout()
    fig.savefig(os.path.join(out, "stress_distribution.png"))
    plt.close(fig)
    print("  wrote stress_distribution.png")


def chart_mtls(data, out):
    """p50 / p99 handshake latency per TLS variant."""
    if not data:
        return
    v = data["variants"]
    order = [("plain_tcp", "Plain TCP", SAGE),
             ("tls13", "TLS 1.3", INK),
             ("mtls13", "mTLS 1.3", AMBER)]
    names = [o[1] for o in order]
    p50 = [v[o[0]]["p50_ms"] for o in order]
    p99 = [v[o[0]]["p99_ms"] for o in order]

    import numpy as np
    x = np.arange(len(names))
    w = 0.36
    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    b1 = ax.bar(x - w / 2, p50, w, label="p50", color=INK, zorder=3)
    b2 = ax.bar(x + w / 2, p99, w, label="p99", color=AMBER, zorder=3)
    for bars in (b1, b2):
        for b in bars:
            ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.3,
                    f"{b.get_height():.1f}", ha="center", va="bottom", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels(names)
    ax.set_ylabel("handshake latency (ms)")
    n = data.get("samples", 0)
    nlabel = f"{n // 1000}k" if n >= 1000 else str(n)
    ax.set_title(f"TLS handshake cost  ·  {nlabel} samples/variant")
    ax.legend(frameon=False, fontsize=10)
    style_axes(ax)
    ax.text(0.0, max(p99) * 0.93, "* plain p99 = TIME_WAIT artefact",
            fontsize=8, color=SAGE, style="italic")
    fig.tight_layout()
    fig.savefig(os.path.join(out, "mtls_latency.png"))
    plt.close(fig)
    print("  wrote mtls_latency.png")


def chart_soak(data, out):
    """Goroutines + heap over time — the flat 'no leak' trend."""
    if not data:
        return
    s = data["samples"]
    t = [x["t_sec"] for x in s]
    g = [x["goroutines"] for x in s]
    h = [x["heap_inuse_mb"] for x in s]

    fig, ax1 = plt.subplots(figsize=(7.5, 4.2))
    ax1.plot(t, g, color=INK, marker="o", markersize=3, linewidth=1.6,
             label="goroutines", zorder=3)
    ax1.set_xlabel("time (s)")
    ax1.set_ylabel("goroutines", color=INK)
    ax1.set_ylim(0, max(g) * 1.4)
    ax1.yaxis.set_major_locator(MaxNLocator(integer=True))

    ax2 = ax1.twinx()
    ax2.plot(t, h, color=AMBER, marker="s", markersize=3, linewidth=1.6,
             label="heap in use (MB)", zorder=3)
    ax2.set_ylabel("heap in use (MB)", color=AMBER)
    ax2.set_ylim(0, max(h) * 1.8)
    ax2.tick_params(axis="y", colors=AMBER)

    ax1.spines["top"].set_visible(False)
    ax1.grid(axis="y", color=GRID, linewidth=0.8)
    ax1.set_axisbelow(True)
    verdict = data.get("verdict", "")
    ax1.set_title("Resource use under sustained load — flat = no leak")
    # combined legend
    l1, lab1 = ax1.get_legend_handles_labels()
    l2, lab2 = ax2.get_legend_handles_labels()
    ax1.legend(l1 + l2, lab1 + lab2, loc="upper center", frameon=False,
               fontsize=10, ncol=2)
    fig.tight_layout()
    fig.savefig(os.path.join(out, "soak_trend.png"))
    plt.close(fig)
    print("  wrote soak_trend.png")


def main():
    json_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "./charts"
    os.makedirs(out_dir, exist_ok=True)
    print(f"reading JSON from {json_dir}, writing PNGs to {out_dir}")
    chart_hetero_p99(load(json_dir, "hetero.json"), out_dir)
    chart_hetero_distribution(load(json_dir, "hetero.json"), out_dir)
    chart_stress(load(json_dir, "stress.json"), out_dir)
    chart_mtls(load(json_dir, "mtls.json"), out_dir)
    chart_soak(load(json_dir, "soak.json"), out_dir)
    print("done.")


if __name__ == "__main__":
    main()
