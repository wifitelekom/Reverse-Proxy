package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
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
	cmd := flag.String("cmd", "", "Komut: kill|metrics|stress|failover|partition")
	nodeID := flag.String("node", "", "Hedef node ID (kill/failover/partition için)")
	count := flag.Int("count", 100, "Stress test için request sayısı")
	duration := flag.Duration("duration", 15*time.Second, "Partition süresi (partition komutu için)")
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

	case "partition":
		if *nodeID == "" {
			fmt.Fprintln(os.Stderr, "partition için -node gerekli")
			os.Exit(1)
		}
		partitionNode(*nodeID, *duration)

	default:
		fmt.Fprintln(os.Stderr, "Kullanım:")
		fmt.Fprintln(os.Stderr, "  simulator -cmd=kill      -node=node-c")
		fmt.Fprintln(os.Stderr, "  simulator -cmd=partition -node=node-b [-duration=15s]")
		fmt.Fprintln(os.Stderr, "  simulator -cmd=metrics")
		fmt.Fprintln(os.Stderr, "  simulator -cmd=stress    [-count=1000]")
		fmt.Fprintln(os.Stderr, "  simulator -cmd=failover  -node=node-c")
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

// partitionNode bir node'u gossip ağından izole eder (UDP paketleri drop edilir).
// Node HTTP isteğlere cevap vermeye devam eder ama gossip'te görünmez olur.
// K2: Network partition → convergence testi için kullanılır.
func partitionNode(nodeID string, duration time.Duration) {
	node, ok := defaultCluster[nodeID]
	if !ok {
		fmt.Fprintf(os.Stderr, "bilinmeyen node: %s\n", nodeID)
		os.Exit(1)
	}

	url := fmt.Sprintf("http://%s/debug/partition?duration=%s", node.ServiceAddr, duration)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "partition başarısız: %v\n", err)
		os.Exit(1)
	}
	resp.Body.Close()
	fmt.Printf("[%s] node gossip'ten izole edildi: %s (süre: %v)\n",
		time.Now().Format("15:04:05.000"), nodeID, duration)
	fmt.Printf("Convergence testi için: simulator -cmd=metrics\n")
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

	stateNames := []string{"ALIVE", "SUSPECT", "DEAD"}

	for nodeID, node := range defaultCluster {
		url := fmt.Sprintf("http://%s/members", node.ServiceAddr)
		resp, err := client.Get(url)
		if err != nil {
			fmt.Printf("[%s] ERİŞİLEMİYOR: %v\n", nodeID, err)
			continue
		}

		var members []memberInfo
		json.NewDecoder(resp.Body).Decode(&members)
		resp.Body.Close()

		fmt.Printf("\n[%s] %d üye görülüyor:\n", nodeID, len(members))
		for _, m := range members {
			stateName := "UNKNOWN"
			if int(m.State) < len(stateNames) {
				stateName = stateNames[m.State]
			}
			fmt.Printf("  - %s @ %s => %s\n", m.ID, m.Addr, stateName)
		}
	}
}

// stressTest LB'ye N adet HTTP request gönderir ve dağılımı raporlar.
// K6: Yük dağılım std sapması < %15 doğrulaması için kullanılır.
// Formül düzeltmesi: gerçek standart sapma = sqrt(varyans), aktif node sayısıyla normalize.
func stressTest(count int) {
	fmt.Printf("=== Stress Test: %d request ===\n", count)

	client := &http.Client{Timeout: 2 * time.Second}
	url := fmt.Sprintf("http://%s/", lbAddr)

	var (
		success int64
		failed  int64
	)

	// Her bilinen node için sayaç
	nodeCounts := make(map[string]*int64)
	for id := range defaultCluster {
		var c int64
		nodeCounts[id] = &c
	}

	var mu sync.Mutex
	unknownCounts := make(map[string]int)

	var wg sync.WaitGroup
	sem := make(chan struct{}, 20)

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

	// Aktif node'ları belirle (en az 1 request alan)
	activeNodes := 0
	total := int64(0)
	for id, counter := range nodeCounts {
		c := atomic.LoadInt64(counter)
		total += c
		fmt.Printf("  %s: %d request\n", id, c)
		if c > 0 {
			activeNodes++
		}
	}
	for id, c := range unknownCounts {
		fmt.Printf("  %s (bilinmeyen): %d request\n", id, c)
	}

	if total > 0 && activeNodes > 0 {
		// Beklenen dağılım: aktif node sayısına göre normalize
		expected := float64(total) / float64(activeNodes)
		fmt.Printf("\nAktif node sayısı: %d\n", activeNodes)
		fmt.Printf("Beklenen dağılım: ~%.0f/node\n", expected)

		// Gerçek standart sapma = sqrt(varyans)
		sumSq := 0.0
		for _, counter := range nodeCounts {
			c := float64(atomic.LoadInt64(counter))
			if c == 0 {
				continue // inactive node'ları hesaba katma
			}
			diff := c - expected
			sumSq += diff * diff
		}
		variance := sumSq / float64(activeNodes)
		stdDev := math.Sqrt(variance) // FIX: karekök alınmıştı eksik

		fmt.Printf("Std sapma: %.2f\n", stdDev)
		cv := (stdDev / expected) * 100 // Coefficient of Variation (%)
		fmt.Printf("Değişim katsayısı (CV): %.1f%% (K6 hedefi: <15%%)\n", cv)
		if cv < 15 {
			fmt.Println("✓ K6 BAŞARILI")
		} else {
			fmt.Printf("✗ K6 BAŞARISIZ (%.1f%% > 15%%)\n", cv)
		}
	}
}

// measureFailover node kill'den sonra failover süresini ölçer.
// K1: Failover < 5s doğrulaması.
// Düzeltme:
//   - start := time.Now() → killNode SONRASINA taşındı
//   - İlk 200 OK yetmez: killed node'a gitmeyen 5 ardışık başarılı yanıt beklenir
func measureFailover(nodeID string) {
	fmt.Printf("=== Failover Testi: %s ===\n", nodeID)

	client := &http.Client{Timeout: 1 * time.Second}
	lbURL := fmt.Sprintf("http://%s/", lbAddr)

	// LB'nin çalıştığını doğrula
	resp, err := client.Get(lbURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "LB erişilemiyor: %v\n", err)
		os.Exit(1)
	}
	resp.Body.Close()
	fmt.Println("LB çalışıyor ✓")

	// Node'u öldür
	fmt.Printf("[%s] %s öldürülüyor...\n", time.Now().Format("15:04:05.000"), nodeID)
	killNode(nodeID)

	// FIX: start timer killNode SONRASI — gossip'in fark etmesi gereken süreyi ölç
	start := time.Now()
	fmt.Println("Failover ölçümü başladı (gossip convergence bekleniyor)...")

	const (
		maxAttempts    = 300                  // max 15s (300 × 50ms)
		pollInterval   = 50 * time.Millisecond
		requiredConsec = 5                    // kaç ardışık başarılı yanıt
	)

	consecutive := 0
	for i := 0; i < maxAttempts; i++ {
		time.Sleep(pollInterval)

		resp, err := client.Get(lbURL)
		if err != nil {
			consecutive = 0
			continue
		}

		// JSON body'den hangi node cevap verdi?
		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()

		respNode, _ := result["node"].(string)

		if resp.StatusCode == http.StatusOK && respNode != nodeID {
			// Başarılı ve killed node'a gitmeyen yanıt
			consecutive++
			if consecutive >= requiredConsec {
				elapsed := time.Since(start)
				fmt.Printf("\n[%s] Failover tamamlandı! (%d ardışık başarılı yanıt)\n",
					time.Now().Format("15:04:05.000"), requiredConsec)
				fmt.Printf("Failover süresi: %v\n", elapsed.Round(time.Millisecond))

				if elapsed < 5*time.Second {
					fmt.Println("✓ K1 BAŞARILI (< 5s)")
				} else {
					fmt.Printf("✗ K1 BAŞARISIZ (%.1fs > 5s)\n", elapsed.Seconds())
				}
				return
			}
		} else {
			// Killed node'a istek gitti veya hata — sayacı sıfırla
			if respNode == nodeID {
				fmt.Printf("  [!] Trafik hâlâ %s'e gidiyor (gossip henüz öğrenmedi)\n", nodeID)
			}
			consecutive = 0
		}
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
