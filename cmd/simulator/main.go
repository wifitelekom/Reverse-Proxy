package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// nodeConfig cluster node bilgileri
type nodeConfig struct {
	ServiceAddr string // HTTP servis adresi
}

var defaultCluster = map[string]nodeConfig{
	"node-a": {ServiceAddr: "localhost:8001"},
	"node-b": {ServiceAddr: "localhost:8002"},
	"node-c": {ServiceAddr: "localhost:8003"},
	"node-d": {ServiceAddr: "localhost:8004"},
}

const lbAddr = "localhost:8080"

func main() {
	cmd := flag.String("cmd", "", "Komut: kill|metrics|stress|failover")
	nodeID := flag.String("node", "", "Hedef node ID (kill/failover için)")
	count := flag.Int("count", 100, "Stress test için request sayısı")
	flag.Parse()

	switch *cmd {
	case "kill":
		if *nodeID == "" {
			fmt.Fprintln(os.Stderr, "kill için -node gerekli")
			os.Exit(1)
		}
		killNode(*nodeID)

	case "metrics":
		collectMetrics()

	case "stress":
		stressTest(*count)

	case "failover":
		if *nodeID == "" {
			fmt.Fprintln(os.Stderr, "failover için -node gerekli")
			os.Exit(1)
		}
		measureFailover(*nodeID)

	default:
		fmt.Fprintln(os.Stderr, "Kullanım:")
		fmt.Fprintln(os.Stderr, "  simulator -cmd=kill -node=node-c")
		fmt.Fprintln(os.Stderr, "  simulator -cmd=metrics")
		fmt.Fprintln(os.Stderr, "  simulator -cmd=stress -count=1000")
		fmt.Fprintln(os.Stderr, "  simulator -cmd=failover -node=node-c")
		flag.Usage()
		os.Exit(1)
	}
}

// killNode bir node'u /debug/kill endpoint'i üzerinden öldürür.
func killNode(nodeID string) {
	node, ok := defaultCluster[nodeID]
	if !ok {
		fmt.Fprintf(os.Stderr, "bilinmeyen node: %s\n", nodeID)
		fmt.Fprintf(os.Stderr, "mevcut node'lar: %s\n", nodeList())
		os.Exit(1)
	}

	url := fmt.Sprintf("http://%s/debug/kill", node.ServiceAddr)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kill başarısız: %v\n", err)
		os.Exit(1)
	}
	resp.Body.Close()
	fmt.Printf("[%s] node öldürüldü: %s\n", time.Now().Format("15:04:05.000"), nodeID)
}

// collectMetrics tüm node'ların membership görüşünü çeker ve karşılaştırır.
// K2: State convergence < 10s doğrulaması için kullanılır.
func collectMetrics() {
	type memberInfo struct {
		ID    string `json:"id"`
		State int32  `json:"state"`
		Addr  string `json:"addr"`
	}

	client := &http.Client{Timeout: 2 * time.Second}
	fmt.Println("=== Membership Görüşleri ===")

	stateMap := map[string]string{
		"0": "ALIVE",
		"1": "SUSPECT",
		"2": "DEAD",
	}

	for nodeID, node := range defaultCluster {
		url := fmt.Sprintf("http://%s/members", node.ServiceAddr)
		resp, err := client.Get(url)
		if err != nil {
			fmt.Printf("[%s] ERIŞILEMIYOR: %v\n", nodeID, err)
			continue
		}

		var members []memberInfo
		json.NewDecoder(resp.Body).Decode(&members)
		resp.Body.Close()

		fmt.Printf("\n[%s] %d üye görülüyor:\n", nodeID, len(members))
		for _, m := range members {
			stateStr, ok := stateMap[fmt.Sprintf("%d", m.State)]
			if !ok {
				stateStr = "UNKNOWN"
			}
			fmt.Printf("  - %s @ %s => %s\n", m.ID, m.Addr, stateStr)
		}
	}
}

// stressTest LB'ye N adet HTTP request gönderir ve dağılımı raporlar.
// K6: Yük dağılım std sapması < %15 doğrulaması için kullanılır.
func stressTest(count int) {
	fmt.Printf("=== Stress Test: %d request ===\n", count)

	client := &http.Client{Timeout: 2 * time.Second}
	url := fmt.Sprintf("http://%s/", lbAddr)

	var (
		success int64
		failed  int64
	)

	nodeCounts := make(map[string]*int64)
	for id := range defaultCluster {
		var c int64
		nodeCounts[id] = &c
	}

	var mu sync.Mutex
	unknownCounts := make(map[string]int)

	var wg sync.WaitGroup
	sem := make(chan struct{}, 20) // 20 concurrent request

	for i := 0; i < count; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			resp, err := client.Get(url)
			if err != nil {
				atomic.AddInt64(&failed, 1)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				atomic.AddInt64(&failed, 1)
				return
			}

			atomic.AddInt64(&success, 1)

			// Hangi node cevap verdi?
			var result map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				return
			}
			if nodeVal, ok := result["node"].(string); ok {
				if counter, found := nodeCounts[nodeVal]; found {
					atomic.AddInt64(counter, 1)
				} else {
					mu.Lock()
					unknownCounts[nodeVal]++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	fmt.Printf("\nSonuçlar:\n")
	fmt.Printf("  Başarılı: %d\n", success)
	fmt.Printf("  Başarısız: %d\n", failed)
	fmt.Printf("\nNode dağılımı:\n")

	total := int64(0)
	for id, counter := range nodeCounts {
		c := atomic.LoadInt64(counter)
		total += c
		fmt.Printf("  %s: %d request\n", id, c)
	}
	for id, c := range unknownCounts {
		fmt.Printf("  %s (bilinmeyen): %d request\n", id, c)
	}

	if total > 0 {
		expected := float64(total) / float64(len(nodeCounts))
		fmt.Printf("\nBeklenen dağılım: ~%.0f/node\n", expected)

		// Std sapma hesapla
		sumSq := 0.0
		for _, counter := range nodeCounts {
			c := float64(atomic.LoadInt64(counter))
			diff := c - expected
			sumSq += diff * diff
		}
		stdDev := 0.0
		if len(nodeCounts) > 0 {
			stdDev = sumSq / float64(len(nodeCounts))
		}
		fmt.Printf("Std sapma (varyans): %.2f\n", stdDev)
		pct := (stdDev / expected) * 100
		fmt.Printf("Std sapma %%: %.1f%% (K6 hedefi: <%d%%)\n", pct, 15)
		if pct < 15 {
			fmt.Println("✓ K6 BAŞARILI")
		} else {
			fmt.Println("✗ K6 BAŞARISIZ")
		}
	}
}

// measureFailover node kill'den sonra failover süresini ölçer.
// K1: Failover < 5s doğrulaması.
func measureFailover(nodeID string) {
	fmt.Printf("=== Failover Testi: %s ===\n", nodeID)

	client := &http.Client{Timeout: 1 * time.Second}
	lbURL := fmt.Sprintf("http://%s/", lbAddr)

	// Önce LB'nin çalıştığını doğrula
	resp, err := client.Get(lbURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "LB erişilemiyor: %v\n", err)
		os.Exit(1)
	}
	resp.Body.Close()
	fmt.Println("LB çalışıyor ✓")

	// Node'u öldür
	fmt.Printf("[%s] %s öldürülüyor...\n", time.Now().Format("15:04:05.000"), nodeID)
	start := time.Now()
	killNode(nodeID)

	// LB'ye request atmaya devam et
	fmt.Println("LB'ye request gönderiliyor, failover bekleniyor...")

	const maxAttempts = 200
	const pollInterval = 50 * time.Millisecond

	for i := 0; i < maxAttempts; i++ {
		time.Sleep(pollInterval)

		resp, err := client.Get(lbURL)
		if err != nil {
			continue
		}

		if resp.StatusCode == http.StatusOK {
			elapsed := time.Since(start)
			resp.Body.Close()
			fmt.Printf("\n[%s] Failover tamamlandı!\n", time.Now().Format("15:04:05.000"))
			fmt.Printf("Failover süresi: %v\n", elapsed.Round(time.Millisecond))

			if elapsed < 5*time.Second {
				fmt.Println("✓ K1 BAŞARILI (< 5s)")
			} else {
				fmt.Printf("✗ K1 BAŞARISIZ (%.1fs > 5s)\n", elapsed.Seconds())
			}
			return
		}
		resp.Body.Close()
	}

	elapsed := time.Since(start)
	fmt.Printf("Failover %v içinde tamamlanamadı ✗\n", elapsed.Round(time.Millisecond))
}

// nodeList mevcut node isimlerini virgülle döner.
func nodeList() string {
	names := make([]string, 0, len(defaultCluster))
	for id := range defaultCluster {
		names = append(names, id)
	}
	return strings.Join(names, ", ")
}
