package traefik_plugin_hybrid_batching

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config the plugin configuration.
type Config struct {
	PriorityThreshold float64 `json:"priorityThreshold"`
	Alpha             float64 `json:"alpha"`
	Beta              float64 `json:"beta"`
	Gamma             float64 `json:"gamma"`
	C                 float64 `json:"C"`
	Weights           Weights `json:"weights"`
	BatchSizeLimits   Limits  `json:"batchSizeLimits"`
	IntervalLimits    Limits  `json:"intervalLimits"`
}

// Weights for priority calculation.
type Weights struct {
	W1 float64 `json:"w1"`
	W2 float64 `json:"w2"`
	W3 float64 `json:"w3"`
	W4 float64 `json:"w4"`
}

// Limits for batch size and intervals.
type Limits struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

// CreateConfig creates the default plugin configuration.
func CreateConfig() *Config {
	return &Config{
		PriorityThreshold: 10.0,
		Alpha:             1.0,
		Beta:              10.0,
		Gamma:             0.5,
		C:                 100.0,
		Weights: Weights{
			W1: 1.0,
			W2: 1.0,
			W3: 1.0,
			W4: 1.0,
		},
		BatchSizeLimits: Limits{
			Min: 10,
			Max: 100,
		},
		IntervalLimits: Limits{
			Min: 1.0,
			Max: 10.0,
		},
	}
}

// New created a new Hybrid Batching middleware.
func New(ctx context.Context, next http.Handler, config *Config, name string) (http.Handler, error) {
	if config == nil {
		config = CreateConfig()
	}

	hb := &HybridBatching{
		next:              next,
		name:              name,
		config:            config,
		highPriorityQueue: make(chan *http.Request, 1000),
		lowPriorityQueue:  make(chan *http.Request, 1000),
		stopChan:          make(chan struct{}),
		arrivalRates:      make([]float64, 0, 60), // Keep data for the last 60 seconds
		maxSamples:        60,                     // Adjust as needed
	}

	hb.init()

	return hb, nil
}

type HybridBatching struct {
	next   http.Handler
	name   string
	config *Config

	highPriorityQueue chan *http.Request
	lowPriorityQueue  chan *http.Request

	systemLoad     float64
	trafficPattern float64

	totalPriority float64
	eventCount    int

	requestCount int64 // Total requests received in the current interval

	arrivalRates []float64 // Slice to store arrival rates for averaging
	maxSamples   int

	mu sync.Mutex

	stopChan chan struct{}
}

func (hb *HybridBatching) init() {
	go hb.startMetricsCollection()
	go hb.startBatchProcessing()
}
func (hb *HybridBatching) startMetricsCollection() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			hb.updateSystemLoad()
			hb.updateTrafficPattern()
		case <-hb.stopChan:
			ticker.Stop()
			return
		}
	}
}

func (hb *HybridBatching) updateSystemLoad() {

	cpuUsage := getCPUUsage()
	memUsage := getMemoryUsage()

	wCPU := 0.6
	wMem := 0.4

	hb.mu.Lock()
	hb.systemLoad = wCPU*cpuUsage + wMem*memUsage
	hb.mu.Unlock()
}

//	func (hb *HybridBatching) updateTrafficPattern() {
//	    // Implement logic to calculate traffic patterns based on the arrival rate.
//	    // For simplicity, this example sets a static value.
//	    hb.mu.Lock()
//	    hb.trafficPattern = 0.5 // Placeholder value
//	    hb.mu.Unlock()
//	}
func (hb *HybridBatching) ServeHTTP(rw http.ResponseWriter, req *http.Request) {

	atomic.AddInt64(&hb.requestCount, 1)
	receiveTime := strconv.FormatInt(time.Now().UnixNano(), 10)
	req.Header.Add("X-Receive-Time", receiveTime)
	// Classify the event (request)
	priority := hb.classifyEvent(req)

	if priority >= hb.config.PriorityThreshold {
		// High-priority event
		select {
		case hb.highPriorityQueue <- req:
			// Successfully added to the queue
		default:
			// Queue is full, process immediately
			hb.processEvent(req)
		}
	} else {
		// Low-priority event
		hb.mu.Lock()
		hb.totalPriority += priority
		hb.eventCount++
		hb.mu.Unlock()

		select {
		case hb.lowPriorityQueue <- req:
			// Successfully added to the queue
		default:
			// Queue is full, process immediately
			hb.processEvent(req)
		}
	}

	// Send an immediate response to the client if necessary
	// rw.WriteHeader(http.StatusAccepted)
	// rw.Write([]byte("Event received"))
}
func (hb *HybridBatching) classifyEvent(req *http.Request) float64 {
	// Extract event attributes from the request
	eventTypeValue := hb.getEventTypeValue(req.Header.Get("X-Event-Type"))
	urgency, _ := strconv.ParseFloat(req.Header.Get("X-Urgency"), 64)

	hb.mu.Lock()
	L := hb.systemLoad
	T := hb.trafficPattern // Use the updated traffic pattern
	hb.mu.Unlock()

	w := hb.config.Weights

	priority := w.W1*eventTypeValue + w.W2*urgency + w.W3*L + w.W4*T

	return priority
}

func (hb *HybridBatching) getEventTypeValue(eventType string) float64 {
	switch eventType {
	case "transaction":
		return 10.0
	case "logging":
		return 2.0
	case "monitoring":
		return 5.0
	default:
		return 1.0
	}
}
func (hb *HybridBatching) startBatchProcessing() {
	for {
		hb.mu.Lock()
		processingInterval := hb.calculateProcessingInterval()
		hb.mu.Unlock()

		timer := time.NewTimer(time.Duration(processingInterval) * time.Second)

		select {
		case <-timer.C:
			hb.processCurrentBatch()
		case <-hb.stopChan:
			timer.Stop()
			return
		}
	}
}
func (hb *HybridBatching) calculateAveragePriority() float64 {
	hb.mu.Lock()
	defer hb.mu.Unlock()

	if hb.eventCount == 0 {
		return 0.0
	}

	return hb.totalPriority / float64(hb.eventCount)
}
func (hb *HybridBatching) calculateOptimalBatchSize() int {
	hb.mu.Lock()
	P_avg := hb.calculateAveragePriority()
	L := hb.systemLoad
	T := hb.trafficPattern
	hb.mu.Unlock()

	alpha := hb.config.Alpha
	beta := hb.config.Beta
	gamma := hb.config.Gamma

	B_opt := (alpha*P_avg + beta) / (L + gamma*T)
	B_opt = hb.applyBatchSizeConstraints(B_opt)

	return int(B_opt)
}

func (hb *HybridBatching) applyBatchSizeConstraints(B_opt float64) float64 {
	min := hb.config.BatchSizeLimits.Min
	max := hb.config.BatchSizeLimits.Max

	if B_opt < min {
		return min
	} else if B_opt > max {
		return max
	}
	return B_opt
}
func (hb *HybridBatching) calculateProcessingInterval() float64 {
	hb.mu.Lock()
	P_avg := hb.calculateAveragePriority()
	L := hb.systemLoad
	hb.mu.Unlock()

	C := hb.config.C

	I := C / (L + P_avg)
	I = hb.applyIntervalConstraints(I)

	return I
}

func (hb *HybridBatching) applyIntervalConstraints(I float64) float64 {
	min := hb.config.IntervalLimits.Min
	max := hb.config.IntervalLimits.Max

	if I < min {
		return min
	} else if I > max {
		return max
	}
	return I
}
func (hb *HybridBatching) processEvent(req *http.Request) {
	// Process the high-priority event immediately
	// Forward the request to the next handler or service
	forwardTime := strconv.FormatInt(time.Now().UnixNano(), 10)
	req.Header.Add("X-Forward-Time", forwardTime)
	hb.next.ServeHTTP(nil, req)
}

func (hb *HybridBatching) processCurrentBatch() {
	batchSize := hb.calculateOptimalBatchSize()
	events := make([]*http.Request, 0, batchSize)

	hb.mu.Lock()
Loop:
	for i := 0; i < batchSize; i++ {
		select {
		case req := <-hb.lowPriorityQueue:
			// Successfully received a request from the queue
			events = append(events, req)
			priority := hb.classifyEvent(req)
			hb.totalPriority -= priority
			hb.eventCount--
		default:
			// No more requests in the queue, break out of the loop
			break Loop
		}
	}
	hb.mu.Unlock()

	if len(events) > 0 {
		// Process the batch of events
		hb.processBatch(events)
	}
}

func (hb *HybridBatching) processBatch(events []*http.Request) {
	// Process the batch of events
	// This could involve aggregating data or forwarding requests in bulk
	for _, req := range events {
		forwardTime := strconv.FormatInt(time.Now().UnixNano(), 10)
		req.Header.Add("X-Forward-Time", forwardTime)
		hb.next.ServeHTTP(nil, req)
	}
}
func (hb *HybridBatching) updateTrafficPattern() {
	// Get and reset the request count atomically
	count := atomic.SwapInt64(&hb.requestCount, 0)
	intervalInSeconds := 1.0 // Since we're updating every second

	// Calculate arrival rate for the current interval
	currentArrivalRate := float64(count) / intervalInSeconds

	hb.mu.Lock()
	// Add the latest arrival rate to the slice
	hb.arrivalRates = append(hb.arrivalRates, currentArrivalRate)

	// Keep only the last maxSamples arrival rates
	if len(hb.arrivalRates) > hb.maxSamples {
		hb.arrivalRates = hb.arrivalRates[1:]
	}

	// Use calculateArrivalRate() to compute the average
	hb.trafficPattern = hb.calculateArrivalRate()
	hb.mu.Unlock()
}

func (hb *HybridBatching) calculateArrivalRate() float64 {
	if len(hb.arrivalRates) == 0 {
		return 0.0
	}

	sum := 0.0
	for _, rate := range hb.arrivalRates {
		sum += rate
	}

	averageArrivalRate := sum / float64(len(hb.arrivalRates))

	return averageArrivalRate
}

// --------------------------------

func getCPUUsage() float64 {
	// Read CPU stats
	contents, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0
	}

	lines := strings.Split(string(contents), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "cpu ") {
			fields := strings.Fields(line)[1:]
			var total uint64
			var idle uint64

			for i, v := range fields {
				val, _ := strconv.ParseUint(v, 10, 64)
				total += val
				if i == 3 { // idle is the 4th field
					idle = val
				}
			}

			return 100 * float64(total-idle) / float64(total)
		}
	}
	return 0
}

func getMemoryUsage() float64 {
	contents, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}

	var total, available uint64
	lines := strings.Split(string(contents), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		value, _ := strconv.ParseUint(fields[1], 10, 64)
		switch fields[0] {
		case "MemTotal:":
			total = value
		case "MemAvailable:":
			available = value
		}
	}

	used := total - available
	return 100 * float64(used) / float64(total)
}
