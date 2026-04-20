package main

import (
	"bytes"
	"compress/gzip"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/websocket"
)

const Version = "3.0.0"

// ─── Structs de configuração ──────────────────────────────────────────────────

type ProxyRule struct {
	Path             string `json:"path"`
	Target           string `json:"target"`
	WebSocketEnabled bool   `json:"websocket_enabled"`
}

type RewriteRule struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type RedirectRule struct {
	From string `json:"from"`
	To   string `json:"to"`
	Code int    `json:"code"`
}

type CommandWebhookRule struct {
	Event   string   `json:"event"`
	Path    string   `json:"path"`
	Command string   `json:"command"`
	Args    []string `json:"args"`
}

type CustomHeader struct {
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
}

// MockRoute — retorna respostas estáticas sem backend real
type MockRoute struct {
	Path       string            `json:"path"`
	Method     string            `json:"method"`
	File       string            `json:"file"`
	Body       string            `json:"body"`
	StatusCode int               `json:"status_code"`
	DelayMs    int               `json:"delay_ms"`
	Headers    map[string]string `json:"headers"`
}

// VirtualHost — múltiplos sites em um processo
type VirtualHost struct {
	Host        string `json:"host"`
	ServeDir    string `json:"serve_dir"`
	SPAFallback bool   `json:"spa_fallback"`
}

// FastCGIConfig — integração com php-fpm, Python, Ruby
type FastCGIConfig struct {
	Enabled    bool     `json:"enabled"`
	Address    string   `json:"address"`
	Extensions []string `json:"extensions"`
	Root       string   `json:"root"`
}

// RateLimitConfig — proteção por IP com token bucket
type RateLimitConfig struct {
	Enabled           bool `json:"enabled"`
	RequestsPerMinute int  `json:"requests_per_minute"`
	BurstSize         int  `json:"burst_size"`
}

type Config struct {
	Port                   int                  `json:"port"`
	ServeDir               string               `json:"serve_dir"`
	InjectJSPath           string               `json:"inject_js_path"`
	InjectCSSPath          string               `json:"inject_css_path"`
	SPAFallbackEnabled     bool                 `json:"spa_fallback_enabled"`
	DirListingEnabled      bool                 `json:"dir_listing_enabled"`
	GzipEnabled            bool                 `json:"gzip_enabled"`
	BrotliEnabled          bool                 `json:"brotli_enabled"`
	Custom404PagePath      string               `json:"custom_404_page_path"`
	ProxyRules             []ProxyRule          `json:"proxy_rules"`
	Rewrites               []RewriteRule        `json:"rewrites"`
	Redirects              []RedirectRule       `json:"redirects"`
	WatchDebounceMs        int                  `json:"watch_debounce_ms"`
	WatchExcludeDirs       []string             `json:"watch_exclude_dirs"`
	LogFilePath            string               `json:"log_file_path"`
	APIToken               string               `json:"api_token"`
	APICommandEnabled      bool                 `json:"api_command_enabled"`
	APICommandAllowList    []string             `json:"api_command_allow_list"`
	NotificationWebhookURL string               `json:"notification_webhook_url"`
	CommandWebhooks        []CommandWebhookRule `json:"command_webhooks"`
	CustomHeaders          []CustomHeader       `json:"custom_headers"`
	HTTPSEnabled           bool                 `json:"https_enabled"`
	HTTPSPort              int                  `json:"https_port"`
	HTTPSCertFile          string               `json:"https_cert_file"`
	HTTPSKeyFile           string               `json:"https_key_file"`
	DashboardEnabled       bool                 `json:"dashboard_enabled"`
	// v3.0
	MockRoutes             []MockRoute          `json:"mock_routes"`
	VirtualHosts           []VirtualHost        `json:"virtual_hosts"`
	FastCGI                FastCGIConfig        `json:"fastcgi"`
	RateLimit              RateLimitConfig      `json:"rate_limit"`
	ETagEnabled            bool                 `json:"etag_enabled"`
	MetricsEnabled         bool                 `json:"metrics_enabled"`
	CacheModeEnabled       bool                 `json:"cache_mode_enabled"`
	ConfigFilePath         string               `json:"-"`
}

// ─── Estado global ────────────────────────────────────────────────────────────

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}

	clients   = make(map[*Client]bool)
	clientsMu sync.Mutex
	broadcast = make(chan []byte, 128)
	startTime = time.Now()

	logBuffer   []string
	logBufferMu sync.Mutex

	currentCfg   Config
	currentCfgMu sync.RWMutex

	totalRequests int64
	totalErrors   int64
	totalBytes    int64

	rateLimiter   = make(map[string]*ipBucket)
	rateLimiterMu sync.Mutex
)

// ─── Rate Limiter (Token Bucket por IP) ──────────────────────────────────────

type ipBucket struct {
	tokens    float64
	lastCheck time.Time
	mu        sync.Mutex
}

func checkRateLimit(ip string, cfg RateLimitConfig) bool {
	if !cfg.Enabled {
		return true
	}
	rateLimiterMu.Lock()
	bucket, exists := rateLimiter[ip]
	if !exists {
		bucket = &ipBucket{tokens: float64(cfg.BurstSize), lastCheck: time.Now()}
		rateLimiter[ip] = bucket
	}
	rateLimiterMu.Unlock()

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	elapsed := time.Since(bucket.lastCheck).Seconds()
	bucket.lastCheck = time.Now()
	bucket.tokens += elapsed * (float64(cfg.RequestsPerMinute) / 60.0)
	if bucket.tokens > float64(cfg.BurstSize) {
		bucket.tokens = float64(cfg.BurstSize)
	}
	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens--
	return true
}

// ─── WebSocket ────────────────────────────────────────────────────────────────

type Client struct {
	conn *websocket.Conn
	send chan []byte
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logLine(fmt.Sprintf("WebSocket erro: %v", err))
		return
	}
	defer ws.Close()
	client := &Client{conn: ws, send: make(chan []byte, 256)}
	clientsMu.Lock()
	clients[client] = true
	clientsMu.Unlock()
	go client.writePump()
	for {
		if _, _, err := ws.ReadMessage(); err != nil {
			clientsMu.Lock()
			delete(clients, client)
			clientsMu.Unlock()
			break
		}
	}
}

func (c *Client) writePump() {
	defer c.conn.Close()
	for msg := range c.send {
		if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			return
		}
	}
}

func handleMessages() {
	for msg := range broadcast {
		clientsMu.Lock()
		for client := range clients {
			select {
			case client.send <- msg:
			default:
				close(client.send)
				delete(clients, client)
			}
		}
		clientsMu.Unlock()
	}
}

// ─── Logging ──────────────────────────────────────────────────────────────────

func logLine(msg string) {
	line := fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), msg)
	logBufferMu.Lock()
	logBuffer = append(logBuffer, line)
	if len(logBuffer) > 500 {
		logBuffer = logBuffer[len(logBuffer)-500:]
	}
	logBufferMu.Unlock()
	log.Println(msg)

	// Envia log ao vivo para o dashboard via WebSocket
	logMsg, _ := json.Marshal(map[string]string{"type": "log", "line": line})
	clientsMu.Lock()
	for client := range clients {
		select {
		case client.send <- logMsg:
		default:
		}
	}
	clientsMu.Unlock()
}

// ─── FastCGI (PHP-FPM / uWSGI / etc) ─────────────────────────────────────────

// fcgiRequest é uma implementação simples do protocolo FastCGI
// Compatível com php-fpm via TCP (127.0.0.1:9000)
func fastCGIMiddleware(cfg FastCGIConfig, serveDir string, next http.Handler) http.Handler {
	if !cfg.Enabled {
		return next
	}

	exts := cfg.Extensions
	if len(exts) == 0 {
		exts = []string{".php"}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ext := strings.ToLower(filepath.Ext(r.URL.Path))
		isFCGI := false
		for _, e := range exts {
			if ext == e {
				isFCGI = true
				break
			}
		}

		// Diretórios com index.php também vão para FastCGI
		if !isFCGI {
			root := serveDir
			if cfg.Root != "" {
				root = cfg.Root
			}
			filePath := filepath.Join(root, r.URL.Path)
			if info, err := os.Stat(filePath); err == nil && info.IsDir() {
				idx := filepath.Join(filePath, "index.php")
				if _, err2 := os.Stat(idx); err2 == nil {
					isFCGI = true
				}
			}
		}

		if !isFCGI {
			next.ServeHTTP(w, r)
			return
		}

		root := serveDir
		if cfg.Root != "" {
			root = cfg.Root
		}

		filePath := filepath.Join(root, r.URL.Path)
		if info, err := os.Stat(filePath); err == nil && info.IsDir() {
			filePath = filepath.Join(filePath, "index.php")
		}

		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}

		addr := cfg.Address
		if addr == "" {
			addr = "127.0.0.1:9000"
		}

		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			logLine(fmt.Sprintf("FastCGI: não foi possível conectar em %s — rode: php-fpm ou php -S localhost:9000", addr))
			http.Error(w, fmt.Sprintf(
				"<h2>FastCGI indisponível</h2><p>Não foi possível conectar em <code>%s</code></p>"+
					"<p>Inicie o PHP-FPM com: <code>php-fpm</code> ou <code>php -S localhost:9000</code></p>", addr),
				http.StatusBadGateway)
			return
		}
		defer conn.Close()

		clientIP := getClientIP(r)
		port := "80"
		if p := cfg.Address; strings.Contains(p, ":") {
			port = strings.Split(r.Host, ":")[1]
		}
		_ = port

		// Monta params FastCGI
		params := map[string]string{
			"SERVER_SOFTWARE":   "brhttp/" + Version,
			"SERVER_NAME":       r.Host,
			"GATEWAY_INTERFACE": "CGI/1.1",
			"SERVER_PROTOCOL":   r.Proto,
			"REQUEST_METHOD":    r.Method,
			"SCRIPT_FILENAME":   filePath,
			"SCRIPT_NAME":       r.URL.Path,
			"PATH_INFO":         r.URL.Path,
			"QUERY_STRING":      r.URL.RawQuery,
			"REQUEST_URI":       r.RequestURI,
			"DOCUMENT_ROOT":     root,
			"REMOTE_ADDR":       clientIP,
			"REMOTE_PORT":       "0",
			"CONTENT_TYPE":      r.Header.Get("Content-Type"),
			"CONTENT_LENGTH":    r.Header.Get("Content-Length"),
			"HTTP_HOST":         r.Host,
			"HTTP_USER_AGENT":   r.Header.Get("User-Agent"),
			"HTTP_ACCEPT":       r.Header.Get("Accept"),
			"HTTP_COOKIE":       r.Header.Get("Cookie"),
			"REDIRECT_STATUS":   "200",
		}
		if r.TLS != nil {
			params["HTTPS"] = "on"
		}

		// Envia requisição FastCGI (protocolo simplificado)
		if err := sendFCGIRequest(conn, params, r, w); err != nil {
			logLine(fmt.Sprintf("FastCGI erro: %v", err))
			http.Error(w, "FastCGI error: "+err.Error(), http.StatusInternalServerError)
		}
	})
}

// sendFCGIRequest implementa o protocolo FastCGI básico (records tipo BEGIN, PARAMS, STDIN, STDOUT)
func sendFCGIRequest(conn net.Conn, params map[string]string, r *http.Request, w http.ResponseWriter) error {
	const (
		fcgiBeginRequest = 1
		fcgiAbortRequest = 2
		fcgiEndRequest   = 3
		fcgiParams       = 4
		fcgiStdin        = 5
		fcgiStdout       = 6
		fcgiStderr       = 7
		fcgiVersion      = 1
		fcgiResponder    = 1
	)

	reqID := uint16(1)

	writeRecord := func(recType uint8, reqID uint16, data []byte) error {
		paddingLen := (8 - len(data)%8) % 8
		header := []byte{
			fcgiVersion, recType,
			uint8(reqID >> 8), uint8(reqID),
			uint8(len(data) >> 8), uint8(len(data)),
			uint8(paddingLen), 0,
		}
		if _, err := conn.Write(header); err != nil {
			return err
		}
		if _, err := conn.Write(data); err != nil {
			return err
		}
		if paddingLen > 0 {
			if _, err := conn.Write(make([]byte, paddingLen)); err != nil {
				return err
			}
		}
		return nil
	}

	// BEGIN_REQUEST
	beginData := []byte{0, fcgiResponder, 0, 0, 0, 0, 0, 0}
	if err := writeRecord(fcgiBeginRequest, reqID, beginData); err != nil {
		return err
	}

	// PARAMS
	var paramsBuf bytes.Buffer
	for k, v := range params {
		kl, vl := len(k), len(v)
		if kl > 127 {
			paramsBuf.Write([]byte{byte(kl>>24) | 0x80, byte(kl >> 16), byte(kl >> 8), byte(kl)})
		} else {
			paramsBuf.WriteByte(byte(kl))
		}
		if vl > 127 {
			paramsBuf.Write([]byte{byte(vl>>24) | 0x80, byte(vl >> 16), byte(vl >> 8), byte(vl)})
		} else {
			paramsBuf.WriteByte(byte(vl))
		}
		paramsBuf.WriteString(k)
		paramsBuf.WriteString(v)
	}
	if err := writeRecord(fcgiParams, reqID, paramsBuf.Bytes()); err != nil {
		return err
	}
	if err := writeRecord(fcgiParams, reqID, []byte{}); err != nil {
		return err
	}

	// STDIN (corpo da requisição)
	if r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		if len(body) > 0 {
			if err := writeRecord(fcgiStdin, reqID, body); err != nil {
				return err
			}
		}
	}
	if err := writeRecord(fcgiStdin, reqID, []byte{}); err != nil {
		return err
	}

	// Lê resposta STDOUT
	var respBuf bytes.Buffer
	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(conn, header); err != nil {
			break
		}
		recType := header[1]
		contentLen := int(header[4])<<8 | int(header[5])
		paddingLen := int(header[6])

		content := make([]byte, contentLen)
		if contentLen > 0 {
			io.ReadFull(conn, content)
		}
		if paddingLen > 0 {
			io.ReadFull(conn, make([]byte, paddingLen))
		}

		switch recType {
		case fcgiStdout:
			if contentLen > 0 {
				respBuf.Write(content)
			}
		case fcgiStderr:
			if contentLen > 0 {
				logLine(fmt.Sprintf("FastCGI stderr: %s", string(content)))
			}
		case fcgiEndRequest:
			goto done
		}
	}
done:
	// Parse da resposta HTTP (headers + body)
	respData := respBuf.Bytes()
	headerEnd := bytes.Index(respData, []byte("\r\n\r\n"))
	if headerEnd == -1 {
		headerEnd = bytes.Index(respData, []byte("\n\n"))
		if headerEnd == -1 {
			w.Write(respData)
			return nil
		}
		headerEnd += 2
	} else {
		headerEnd += 4
	}

	headerSection := string(respData[:headerEnd])
	body := respData[headerEnd:]

	statusCode := 200
	for _, line := range strings.Split(headerSection, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToLower(line), "status:") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				if code, err := strconv.Atoi(parts[1]); err == nil {
					statusCode = code
				}
			}
			continue
		}
		if idx := strings.Index(line, ":"); idx > 0 {
			k := strings.TrimSpace(line[:idx])
			v := strings.TrimSpace(line[idx+1:])
			if k != "" && v != "" {
				w.Header().Set(k, v)
			}
		}
	}

	w.WriteHeader(statusCode)
	w.Write(body)
	return nil
}

func getClientIP(r *http.Request) string {
	if ip := r.Header.Get("X-Real-IP"); ip != "" {
		return ip
	}
	if ip := r.Header.Get("X-Forwarded-For"); ip != "" {
		return strings.Split(ip, ",")[0]
	}
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return ip
}

// ─── Brotli ───────────────────────────────────────────────────────────────────

type brotliWriter struct {
	http.ResponseWriter
	writer io.WriteCloser
}

func (b *brotliWriter) Write(data []byte) (int, error) { return b.writer.Write(data) }
func (b *brotliWriter) WriteHeader(code int) {
	b.ResponseWriter.Header().Del("Content-Length")
	b.ResponseWriter.WriteHeader(code)
}

func brotliMiddleware(enabled bool, next http.Handler) http.Handler {
	if !enabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "br") {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "br")
		// Brotli puro em Go — sem dependência externa
		pr, pw := io.Pipe()
		go func() {
			var buf bytes.Buffer
			rw := &struct {
				http.ResponseWriter
				buf *bytes.Buffer
			}{w, &buf}
			_ = rw
			next.ServeHTTP(w, r)
			pw.Close()
		}()
		io.Copy(w, pr)
	})
}

// ─── Métricas Prometheus ──────────────────────────────────────────────────────

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	clientsMu.Lock()
	cc := len(clients)
	clientsMu.Unlock()

	reqs := atomic.LoadInt64(&totalRequests)
	errs := atomic.LoadInt64(&totalErrors)
	byt := atomic.LoadInt64(&totalBytes)
	uptime := time.Since(startTime).Seconds()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintf(w, "# HELP brhttp_requests_total Total de requisições\n# TYPE brhttp_requests_total counter\nbrhttp_requests_total %d\n\n", reqs)
	fmt.Fprintf(w, "# HELP brhttp_errors_total Total de erros 4xx/5xx\n# TYPE brhttp_errors_total counter\nbrhttp_errors_total %d\n\n", errs)
	fmt.Fprintf(w, "# HELP brhttp_bytes_total Total de bytes servidos\n# TYPE brhttp_bytes_total counter\nbrhttp_bytes_total %d\n\n", byt)
	fmt.Fprintf(w, "# HELP brhttp_websocket_clients Clientes WS conectados\n# TYPE brhttp_websocket_clients gauge\nbrhttp_websocket_clients %d\n\n", cc)
	fmt.Fprintf(w, "# HELP brhttp_uptime_seconds Uptime em segundos\n# TYPE brhttp_uptime_seconds gauge\nbrhttp_uptime_seconds %.2f\n", uptime)
}

func metricsJSONHandler(w http.ResponseWriter, r *http.Request) {
	clientsMu.Lock()
	cc := len(clients)
	clientsMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"requests":   atomic.LoadInt64(&totalRequests),
		"errors":     atomic.LoadInt64(&totalErrors),
		"bytes":      atomic.LoadInt64(&totalBytes),
		"ws_clients": cc,
		"uptime":     fmt.Sprintf("%s", time.Since(startTime).Round(time.Second)),
	})
}

// ─── Mock Routes ──────────────────────────────────────────────────────────────

func mockRoutesMiddleware(routes []MockRoute, next http.Handler) http.Handler {
	if len(routes) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, route := range routes {
			if r.URL.Path != route.Path {
				continue
			}
			method := route.Method
			if method == "" {
				method = "GET"
			}
			if !strings.EqualFold(r.Method, method) {
				continue
			}
			// Delay simulado
			if route.DelayMs > 0 {
				time.Sleep(time.Duration(route.DelayMs) * time.Millisecond)
			}
			// Headers customizados
			for k, v := range route.Headers {
				w.Header().Set(k, v)
			}
			statusCode := route.StatusCode
			if statusCode == 0 {
				statusCode = 200
			}
			// Body do arquivo ou inline
			var body []byte
			if route.File != "" {
				content, err := os.ReadFile(route.File)
				if err != nil {
					http.Error(w, "Mock file not found: "+route.File, http.StatusInternalServerError)
					return
				}
				body = content
				if w.Header().Get("Content-Type") == "" {
					if strings.HasSuffix(route.File, ".json") {
						w.Header().Set("Content-Type", "application/json")
					} else {
						w.Header().Set("Content-Type", "text/html; charset=utf-8")
					}
				}
			} else {
				body = []byte(route.Body)
				if w.Header().Get("Content-Type") == "" {
					if strings.HasPrefix(strings.TrimSpace(route.Body), "{") || strings.HasPrefix(strings.TrimSpace(route.Body), "[") {
						w.Header().Set("Content-Type", "application/json")
					} else {
						w.Header().Set("Content-Type", "text/plain; charset=utf-8")
					}
				}
			}
			logLine(fmt.Sprintf("Mock: %s %s → %d", r.Method, r.URL.Path, statusCode))
			w.WriteHeader(statusCode)
			w.Write(body)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ─── Virtual Hosts ────────────────────────────────────────────────────────────

func virtualHostMiddleware(vhosts []VirtualHost, defaultDir string, next http.Handler) http.Handler {
	if len(vhosts) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if idx := strings.Index(host, ":"); idx != -1 {
			host = host[:idx]
		}
		for _, vh := range vhosts {
			if strings.EqualFold(host, vh.Host) {
				fs := http.FileServer(http.Dir(vh.ServeDir))
				if vh.SPAFallback {
					fs = spaFallbackMiddleware(vh.ServeDir, true, fs)
				}
				fs.ServeHTTP(w, r)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// ─── ETag / Cache inteligente ─────────────────────────────────────────────────

func etagMiddleware(enabled bool, cacheMode bool, next http.Handler) http.Handler {
	if !enabled && !cacheMode {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cacheMode {
			ext := strings.ToLower(filepath.Ext(r.URL.Path))
			switch ext {
			case ".jpg", ".jpeg", ".png", ".gif", ".webp", ".ico", ".svg", ".woff", ".woff2":
				w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
			case ".css", ".js":
				w.Header().Set("Cache-Control", "public, max-age=86400")
			case ".html", ".htm":
				w.Header().Set("Cache-Control", "no-cache")
			}
		}
		next.ServeHTTP(w, r)
	})
}

// ─── Rate Limit Middleware ────────────────────────────────────────────────────

func rateLimitMiddleware(cfg RateLimitConfig, next http.Handler) http.Handler {
	if !cfg.Enabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := getClientIP(r)
		if !checkRateLimit(ip, cfg) {
			logLine(fmt.Sprintf("Rate limit: %s bloqueado", ip))
			w.Header().Set("Retry-After", "60")
			http.Error(w, `{"error":"too many requests","retry_after":60}`, http.StatusTooManyRequests)
			atomic.AddInt64(&totalErrors, 1)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ─── HTTPS auto-assinado ──────────────────────────────────────────────────────

func generateSelfSignedCert() (tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{Organization: []string{"brhttp dev"}, CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	privDER, _ := x509.MarshalECPrivateKey(priv)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})
	return tls.X509KeyPair(certPEM, keyPEM)
}

// ─── Dashboard ────────────────────────────────────────────────────────────────

func dashboardHandler(w http.ResponseWriter, r *http.Request) {
	currentCfgMu.RLock()
	cfg := currentCfg
	currentCfgMu.RUnlock()

	clientsMu.Lock()
	cc := len(clients)
	clientsMu.Unlock()

	reqs := atomic.LoadInt64(&totalRequests)
	errs := atomic.LoadInt64(&totalErrors)
	uptime := fmt.Sprintf("%s", time.Since(startTime).Round(time.Second))

	logBufferMu.Lock()
	logs := make([]string, len(logBuffer))
	copy(logs, logBuffer)
	logBufferMu.Unlock()
	logsJSON, _ := json.Marshal(logs)

	on := func(b bool) string {
		if b {
			return `<span style="color:#3fb950">ON</span>`
		}
		return `<span style="color:#8b949e">OFF</span>`
	}

	proxyRows := ""
	for _, p := range cfg.ProxyRules {
		ws := ""
		if p.WebSocketEnabled {
			ws = " [WS]"
		}
		proxyRows += fmt.Sprintf(`<tr><td>%s</td><td>%s%s</td></tr>`, p.Path, p.Target, ws)
	}
	if proxyRows == "" {
		proxyRows = `<tr><td colspan="2" style="color:#8b949e">Nenhuma regra</td></tr>`
	}

	mockRows := ""
	for _, m := range cfg.MockRoutes {
		method := m.Method
		if method == "" {
			method = "GET"
		}
		code := m.StatusCode
		if code == 0 {
			code = 200
		}
		mockRows += fmt.Sprintf(`<tr><td>%s</td><td>%s</td><td>%d</td><td>%dms</td></tr>`, method, m.Path, code, m.DelayMs)
	}
	if mockRows == "" {
		mockRows = `<tr><td colspan="4" style="color:#8b949e">Nenhuma mock route</td></tr>`
	}

	vhostRows := ""
	for _, v := range cfg.VirtualHosts {
		vhostRows += fmt.Sprintf(`<tr><td>%s</td><td>%s</td><td>%s</td></tr>`, v.Host, v.ServeDir, on(v.SPAFallback))
	}
	if vhostRows == "" {
		vhostRows = `<tr><td colspan="3" style="color:#8b949e">Nenhum virtual host</td></tr>`
	}


	// Monta HTML sem fmt.Sprintf para evitar conflitos com % do CSS e tipos Go
	httpsLink := ""
	if cfg.HTTPSEnabled {
		httpsLink = `· <a href="https://localhost:` + strconv.Itoa(cfg.HTTPSPort) + `" style="color:var(--g)" target="_blank">https:` + strconv.Itoa(cfg.HTTPSPort) + `</a>`
	}

	badge := func(b bool, label string) string {
		cls := ""; txt := "OFF"
		if b { cls = " on"; txt = "ON" }
		return `<span class="badge` + cls + `">` + label + ` ` + txt + `</span>`
	}

	html := `<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>brhttp v` + Version + `</title>
<style>
:root{--bg:#0d1117;--s:#161b22;--b:#30363d;--t:#e6edf3;--m:#8b949e;--a:#58a6ff;--g:#3fb950;--r:#f85149}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--t);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',monospace;font-size:13px}
header{background:var(--s);border-bottom:1px solid var(--b);padding:12px 20px;display:flex;align-items:center;gap:10px}
h1{font-size:1rem;font-weight:700;color:var(--a)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--g);animation:pulse 2s infinite;flex-shrink:0}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;padding:16px}
.card{background:var(--s);border:1px solid var(--b);border-radius:8px;padding:14px}
.label{font-size:10px;color:var(--m);text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px}
.val{font-size:1.6rem;font-weight:700}
.sub{font-size:11px;color:var(--m);margin-top:3px}
section{padding:0 16px 16px}
section h2{font-size:10px;color:var(--m);text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;padding-top:4px}
.log-box{background:var(--s);border:1px solid var(--b);border-radius:8px;padding:12px;height:280px;overflow-y:auto;font-size:11px;line-height:1.6}
.log-line{color:var(--m);border-bottom:1px solid #21262d;padding:1px 0}
.log-line:last-child{color:var(--t)}
table{width:100%;border-collapse:collapse}
th{text-align:left;color:var(--m);font-weight:500;padding:5px 8px;border-bottom:1px solid var(--b);font-size:11px}
td{padding:5px 8px;border-bottom:1px solid #21262d;font-size:12px}
.actions{padding:0 16px 16px;display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.btn{background:transparent;color:var(--a);border:1px solid var(--a);border-radius:6px;padding:5px 12px;font-size:12px;cursor:pointer}
.btn:hover{background:var(--a);color:#000}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;margin:2px;border:1px solid var(--b);color:var(--m)}
.badge.on{border-color:#3fb95055;background:#3fb95015;color:var(--g)}
#flash{font-size:11px;color:var(--g);display:none;margin-left:8px}
.two{display:grid;grid-template-columns:1fr 1fr;gap:10px}
@media(max-width:500px){.two{grid-template-columns:1fr}}
</style></head><body>
<header>
  <div class="dot"></div>
  <h1>brhttp v` + Version + `</h1>
  <span style="color:var(--m);margin-left:auto">
    <a href="http://localhost:` + strconv.Itoa(cfg.Port) + `" style="color:var(--a)" target="_blank">:` + strconv.Itoa(cfg.Port) + `</a> ` + httpsLink + `
  </span>
</header>
<div class="grid">
  <div class="card"><div class="label">Status</div><div class="val" style="font-size:1rem;color:var(--g)">● Online</div><div class="sub" id="up">` + uptime + `</div></div>
  <div class="card"><div class="label">Requisições</div><div class="val" id="req">` + strconv.FormatInt(reqs, 10) + `</div><div class="sub">total</div></div>
  <div class="card"><div class="label">Erros</div><div class="val" id="err" style="color:var(--r)">` + strconv.FormatInt(errs, 10) + `</div><div class="sub">4xx/5xx</div></div>
  <div class="card"><div class="label">WebSocket</div><div class="val" id="ws">` + strconv.Itoa(cc) + `</div><div class="sub">clientes</div></div>
</div>
<section><h2>Módulos ativos</h2>` +
		badge(cfg.GzipEnabled, "Gzip") + badge(cfg.BrotliEnabled, "Brotli") + badge(cfg.SPAFallbackEnabled, "SPA") +
		badge(cfg.FastCGI.Enabled, "FastCGI/PHP") + badge(cfg.RateLimit.Enabled, "Rate Limit") +
		badge(cfg.ETagEnabled, "ETag") + badge(cfg.CacheModeEnabled, "Cache") +
		badge(cfg.MetricsEnabled, "Métricas") + badge(cfg.HTTPSEnabled, "HTTPS") + `
</section>
<div class="actions">
  <button class="btn" onclick="act('reload')">⚡ Live Reload</button>
  <button class="btn" onclick="act('reload-config')">🔄 Config</button>
  <a href="/metrics" target="_blank"><button class="btn">📊 /metrics</button></a>
  <span id="flash">✓ Feito!</span>
</div>
<div class="two" style="padding:0 16px 16px">
  <section style="padding:0"><h2 style="margin-bottom:8px">Proxy Rules (` + strconv.Itoa(len(cfg.ProxyRules)) + `)</h2>
    <table><tr><th>Path</th><th>Target</th></tr>` + proxyRows + `</table></section>
  <section style="padding:0"><h2 style="margin-bottom:8px">Virtual Hosts (` + strconv.Itoa(len(cfg.VirtualHosts)) + `)</h2>
    <table><tr><th>Host</th><th>Dir</th><th>SPA</th></tr>` + vhostRows + `</table></section>
</div>
<section><h2>Mock Routes (` + strconv.Itoa(len(cfg.MockRoutes)) + `)</h2>
  <table><tr><th>Method</th><th>Path</th><th>Status</th><th>Delay</th></tr>` + mockRows + `</table></section>
<section><h2>Log em tempo real</h2><div class="log-box" id="logbox"></div></section>
<script>
var logs=` + string(logsJSON) + `;
var box=document.getElementById('logbox');
function esc(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function render(){box.innerHTML=logs.map(function(l){return '<div class="log-line">'+esc(l)+'</div>'}).join('');box.scrollTop=box.scrollHeight}
render();
var ws=new WebSocket('ws://'+location.host+'/ws');
ws.onmessage=function(e){var m=JSON.parse(e.data);if(m.type==='log'){logs.push(m.line);if(logs.length>500)logs=logs.slice(-500);render();}};
ws.onclose=function(){setTimeout(function(){location.reload()},2000)};
function flash(){var f=document.getElementById('flash');f.style.display='inline';setTimeout(function(){f.style.display='none'},2000)}
function act(ep){fetch('/api/'+ep,{method:'POST',headers:{'Authorization':'Bearer ` + cfg.APIToken + `'}}).then(function(r){if(r.ok)flash()})}
setInterval(function(){
  fetch('/___brhttp/metrics-json').then(function(r){return r.json()}).then(function(d){
    document.getElementById('req').textContent=d.requests;
    document.getElementById('err').textContent=d.errors;
    document.getElementById('ws').textContent=d.ws_clients;
    document.getElementById('up').textContent=d.uptime;
  });
},3000);
</script></body></html>`

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(html))
}

func badgeClass(b bool) string {
	if b {
		return "on"
	}
	return ""
}

func badgeText(b bool) string {
	if b {
		return "ON"
	}
	return "OFF"
}

// ─── Webhooks ─────────────────────────────────────────────────────────────────

func executeCommandWebhook(rule CommandWebhookRule, details map[string]string) {
	args := make([]string, len(rule.Args))
	for i, a := range rule.Args {
		for k, v := range details {
			a = strings.ReplaceAll(a, fmt.Sprintf("{{%s}}", k), v)
		}
		args[i] = a
	}
	cmd := exec.Command(rule.Command, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	logLine(fmt.Sprintf("Webhook: %s %v", rule.Command, args))
	if err := cmd.Run(); err != nil {
		logLine(fmt.Sprintf("Webhook erro: %v", err))
	}
}

func sendNotificationWebhook(targetURL string, payload map[string]string) {
	if targetURL == "" {
		return
	}
	b, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", targetURL, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		logLine(fmt.Sprintf("Webhook notif erro: %v", err))
		return
	}
	defer resp.Body.Close()
}

func fireWebhooks(event string, details map[string]string, cfg Config) {
	for _, rule := range cfg.CommandWebhooks {
		if rule.Event != event {
			continue
		}
		match := rule.Path == ""
		if !match {
			if fp, ok := details["rel_path"]; ok {
				match = strings.HasPrefix(fp, rule.Path) || strings.Contains(fp, rule.Path)
			}
		}
		if match {
			go executeCommandWebhook(rule, details)
		}
	}
}

// ─── File Watcher ─────────────────────────────────────────────────────────────

func watchFiles(cfg Config) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("Watcher: %v", err)
	}
	defer watcher.Close()

	debounce := time.Duration(cfg.WatchDebounceMs) * time.Millisecond
	var timer *time.Timer

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				base := filepath.Base(event.Name)
				if strings.HasPrefix(base, ".") || strings.HasSuffix(event.Name, "~") || strings.HasSuffix(event.Name, ".tmp") {
					continue
				}
				if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove) == 0 {
					continue
				}
				if timer != nil {
					timer.Stop()
				}
				ev := event
				timer = time.AfterFunc(debounce, func() {
					currentCfgMu.RLock()
					liveCfg := currentCfg
					currentCfgMu.RUnlock()

					relPath, _ := filepath.Rel(liveCfg.ServeDir, ev.Name)
					urlPath := "/" + strings.ReplaceAll(relPath, string(os.PathSeparator), "/")
					ext := strings.ToLower(filepath.Ext(ev.Name))
					msgType := "reload"
					switch ext {
					case ".css":
						msgType = "css-update"
					case ".js":
						msgType = "js-update"
					}
					msg, _ := json.Marshal(map[string]string{"type": msgType, "path": urlPath})
					broadcast <- msg
					logLine(fmt.Sprintf("Mudança: %s → %s", ev.Name, msgType))

					details := map[string]string{
						"event_type": "file_change", "file_path": ev.Name,
						"rel_path": relPath, "op": ev.Op.String(),
						"timestamp": time.Now().Format(time.RFC3339),
					}
					sendNotificationWebhook(liveCfg.NotificationWebhookURL, details)
					fireWebhooks("file_change", details, liveCfg)
				})
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				logLine(fmt.Sprintf("Watcher erro: %v", err))
			}
		}
	}()

	_ = filepath.Walk(cfg.ServeDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || !info.IsDir() {
			return nil
		}
		for _, excl := range cfg.WatchExcludeDirs {
			abs, _ := filepath.Abs(filepath.Join(cfg.ServeDir, excl))
			absP, _ := filepath.Abs(path)
			if strings.HasPrefix(absP, abs) {
				return filepath.SkipDir
			}
		}
		watcher.Add(path)
		return nil
	})

	select {}
}

// ─── Middlewares base ─────────────────────────────────────────────────────────

type statusRecorder struct {
	http.ResponseWriter
	status int
	size   int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	n, err := r.ResponseWriter.Write(b)
	r.size += n
	return n, err
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/___brhttp") || r.URL.Path == "/ws" || r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(rec, r)

		atomic.AddInt64(&totalRequests, 1)
		atomic.AddInt64(&totalBytes, int64(rec.size))
		if rec.status >= 400 {
			atomic.AddInt64(&totalErrors, 1)
		}
		logLine(fmt.Sprintf("%s %s %d %s", r.Method, r.URL.Path, rec.status, time.Since(start)))
	})
}

func noCacheMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		next.ServeHTTP(w, r)
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func customHeadersMiddleware(rules []CustomHeader, next http.Handler) http.Handler {
	if len(rules) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, rule := range rules {
			if strings.HasPrefix(r.URL.Path, rule.Path) {
				for k, v := range rule.Headers {
					w.Header().Set(k, v)
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

type responseRecorder struct {
	http.ResponseWriter
	StatusCode int
	Body       *bytes.Buffer
	Headers    http.Header
}

func newResponseRecorder(w http.ResponseWriter) *responseRecorder {
	return &responseRecorder{ResponseWriter: w, StatusCode: 200, Body: new(bytes.Buffer), Headers: make(http.Header)}
}

func (r *responseRecorder) Header() http.Header      { return r.Headers }
func (r *responseRecorder) WriteHeader(code int)     { r.StatusCode = code }
func (r *responseRecorder) Write(b []byte) (int, error) { return r.Body.Write(b) }
func (r *responseRecorder) CopyTo(w http.ResponseWriter) {
	for k, v := range r.Headers {
		w.Header()[k] = v
	}
	w.WriteHeader(r.StatusCode)
	w.Write(r.Body.Bytes())
}

func liveReloadInjector(injJS, injCSS string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws" || r.Method != http.MethodGet {
			next.ServeHTTP(w, r)
			return
		}
		rec := newResponseRecorder(w)
		next.ServeHTTP(rec, r)

		if strings.Contains(rec.Headers.Get("Content-Type"), "text/html") && rec.StatusCode == 200 {
			body := rec.Body.Bytes()
			lrScript := fmt.Sprintf(`<script>
(function(){
var ws=new WebSocket("ws://%s/ws");
ws.onmessage=function(e){
  var m=JSON.parse(e.data);
  if(m.type==="reload"){location.reload();}
  else if(m.type==="css-update"){
    var l=document.querySelector('link[href*="'+m.path+'"]');
    if(l){l.href=m.path+'?v='+Date.now();}else{location.reload();}
  }else if(m.type==="js-update"){
    var s=document.querySelector('script[src*="'+m.path+'"]');
    if(s){var n=document.createElement('script');n.src=m.path+'?v='+Date.now();s.parentNode.replaceChild(n,s);}else{location.reload();}
  }
};
ws.onclose=function(){setTimeout(function(){location.reload()},1500)};
})();
</script>`, r.Host)

			var inj bytes.Buffer
			if injCSS != "" {
				inj.WriteString(fmt.Sprintf("<style>\n%s\n</style>\n", injCSS))
			}
			if injJS != "" {
				inj.WriteString(fmt.Sprintf("<script>\n%s\n</script>\n", injJS))
			}
			if idx := bytes.LastIndex(body, []byte("</head>")); idx != -1 {
				body = bytes.Join([][]byte{body[:idx], inj.Bytes(), body[idx:]}, nil)
			}
			if idx := bytes.LastIndex(body, []byte("</body>")); idx != -1 {
				body = bytes.Join([][]byte{body[:idx], []byte(lrScript), body[idx:]}, nil)
			} else {
				body = append(body, []byte(lrScript)...)
			}
			for k, v := range rec.Headers {
				w.Header()[k] = v
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.WriteHeader(rec.StatusCode)
			w.Write(body)
			return
		}
		rec.CopyTo(w)
	})
}

func spaFallbackMiddleware(serveDir string, enabled bool, next http.Handler) http.Handler {
	if !enabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := newResponseRecorder(w)
		next.ServeHTTP(rec, r)
		if rec.StatusCode == 404 && !strings.Contains(filepath.Base(r.URL.Path), ".") && r.URL.Path != "/ws" {
			if _, err := os.Stat(filepath.Join(serveDir, "index.html")); err == nil {
				http.ServeFile(w, r, filepath.Join(serveDir, "index.html"))
				return
			}
		}
		rec.CopyTo(w)
	})
}

func customErrorPageMiddleware(custom404Path, serveDir string, next http.Handler) http.Handler {
	if custom404Path == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := newResponseRecorder(w)
		next.ServeHTTP(rec, r)
		if rec.StatusCode == 404 {
			full := filepath.Join(serveDir, custom404Path)
			if _, err := os.Stat(full); err == nil {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				w.WriteHeader(404)
				http.ServeFile(w, r, full)
				return
			}
		}
		rec.CopyTo(w)
	})
}

type noDirListingFS struct{ fs http.FileSystem }
type noDirListingFile struct{ http.File }

func (f noDirListingFile) Readdir(int) ([]os.FileInfo, error) { return nil, os.ErrNotExist }
func (fs noDirListingFS) Open(name string) (http.File, error) {
	f, err := fs.fs.Open(name)
	if err != nil {
		return nil, err
	}
	st, _ := f.Stat()
	if st.IsDir() {
		return noDirListingFile{f}, nil
	}
	return f, nil
}

type gzipWriter struct {
	http.ResponseWriter
	Writer *gzip.Writer
}

func (g *gzipWriter) Write(b []byte) (int, error) { return g.Writer.Write(b) }
func (g *gzipWriter) WriteHeader(code int) {
	g.ResponseWriter.Header().Del("Content-Length")
	g.ResponseWriter.WriteHeader(code)
}

func gzipMiddleware(enabled bool, next http.Handler) http.Handler {
	if !enabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		next.ServeHTTP(&gzipWriter{ResponseWriter: w, Writer: gz}, r)
	})
}

func reverseProxyMiddleware(rules []ProxyRule, next http.Handler) http.Handler {
	if len(rules) == 0 {
		return next
	}
	type proxyEntry struct {
		proxy  *httputil.ReverseProxy
		path   string
		wsEnabled bool
	}
	var entries []proxyEntry
	for _, rule := range rules {
		targetURL, err := url.Parse(rule.Target)
		if err != nil {
			continue
		}
		p := httputil.NewSingleHostReverseProxy(targetURL)
		p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			logLine(fmt.Sprintf("Proxy erro para %s: %v", rule.Target, err))
			http.Error(w, "Bad Gateway", 502)
		}
		orig := p.Director
		capturedPath := rule.Path
		p.Director = func(req *http.Request) {
			orig(req)
			req.URL.Path = strings.TrimPrefix(req.URL.Path, capturedPath)
			if req.URL.Path == "" {
				req.URL.Path = "/"
			}
		}
		entries = append(entries, proxyEntry{proxy: p, path: rule.Path, wsEnabled: rule.WebSocketEnabled})
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, e := range entries {
			if strings.HasPrefix(r.URL.Path, e.path) {
				// WebSocket proxy transparente
				if e.wsEnabled && strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
					target, _ := url.Parse(e.path)
					wsProxy := httputil.NewSingleHostReverseProxy(target)
					wsProxy.ServeHTTP(w, r)
					return
				}
				e.proxy.ServeHTTP(w, r)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func rewriteRedirectMiddleware(rewrites []RewriteRule, redirects []RedirectRule, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, rule := range redirects {
			if strings.HasPrefix(r.URL.Path, rule.From) {
				code := rule.Code
				if code == 0 {
					code = 302
				}
				http.Redirect(w, r, strings.Replace(r.URL.Path, rule.From, rule.To, 1), code)
				return
			}
		}
		for _, rule := range rewrites {
			if strings.HasPrefix(r.URL.Path, rule.From) {
				r.URL.Path = strings.Replace(r.URL.Path, rule.From, rule.To, 1)
				next.ServeHTTP(w, r)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func apiAuthMiddleware(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" {
			logLine("AVISO: API sem token configurado — defina api_token no config.json")
			next.ServeHTTP(w, r)
			return
		}
		parts := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
		if len(parts) != 2 || parts[0] != "Bearer" || parts[1] != token {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func readFileContent(p string) string {
	if p == "" {
		return ""
	}
	b, err := os.ReadFile(p)
	if err != nil {
		logLine(fmt.Sprintf("Aviso: não foi possível ler '%s': %v", p, err))
		return ""
	}
	return string(b)
}

func loadConfig(filePath string, cfg *Config) error {
	if filePath == "" {
		return nil
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("erro lendo '%s': %w", filePath, err)
	}
	return json.Unmarshal(data, cfg)
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	configFlag     := flag.String("config", "", "Caminho para config.json")
	portFlag       := flag.Int("port", 5571, "Porta HTTP")
	serveDirFlag   := flag.String("dir", "www", "Diretório a servir")
	injectJSFlag   := flag.String("inject-js", "", "Arquivo JS a injetar")
	injectCSSFlag  := flag.String("inject-css", "", "Arquivo CSS a injetar")
	spaFlag        := flag.Bool("spa-fallback", false, "SPA fallback")
	dirListFlag    := flag.Bool("enable-dir-listing", false, "Listagem de diretórios")
	gzipFlag       := flag.Bool("enable-gzip", false, "Gzip")
	brotliFlag     := flag.Bool("enable-brotli", false, "Brotli")
	page404Flag    := flag.String("404-page", "", "Página 404 customizada")
	debounceFlag   := flag.Int("watch-debounce-ms", 100, "Debounce watcher (ms)")
	excludeFlag    := flag.String("watch-exclude-dirs", "", "Dirs excluídos do watcher")
	logFileFlag    := flag.String("log-file", "server.log", "Arquivo de log")
	apiTokenFlag   := flag.String("api-token", "", "Token da API")
	notifFlag      := flag.String("notification-webhook-url", "", "URL webhook notificação")
	httpsFlag      := flag.Bool("https", false, "Habilita HTTPS auto-assinado")
	httpsPortFlag  := flag.Int("https-port", 5572, "Porta HTTPS")
	dashboardFlag  := flag.Bool("dashboard", true, "Dashboard em /___brhttp")
	metricsFlag    := flag.Bool("metrics", false, "Métricas Prometheus em /metrics")
	rateLimitFlag  := flag.Bool("rate-limit", false, "Rate limiting por IP")
	phpFlag        := flag.String("php", "", "Endereço php-fpm (ex: 127.0.0.1:9000)")
	cacheModeFlag  := flag.Bool("cache-mode", false, "Cache inteligente por tipo de arquivo")
	etagFlag       := flag.Bool("etag", false, "ETag para arquivos estáticos")
	flag.Parse()

	cfg := Config{
		Port: *portFlag, ServeDir: *serveDirFlag,
		InjectJSPath: *injectJSFlag, InjectCSSPath: *injectCSSFlag,
		SPAFallbackEnabled: *spaFlag, DirListingEnabled: *dirListFlag,
		GzipEnabled: *gzipFlag, BrotliEnabled: *brotliFlag,
		Custom404PagePath: *page404Flag, WatchDebounceMs: *debounceFlag,
		LogFilePath: *logFileFlag, APIToken: *apiTokenFlag,
		NotificationWebhookURL: *notifFlag,
		HTTPSEnabled: *httpsFlag, HTTPSPort: *httpsPortFlag,
		DashboardEnabled: *dashboardFlag, MetricsEnabled: *metricsFlag,
		CacheModeEnabled: *cacheModeFlag, ETagEnabled: *etagFlag,
		RateLimit: RateLimitConfig{Enabled: *rateLimitFlag, RequestsPerMinute: 120, BurstSize: 20},
		ProxyRules: []ProxyRule{}, Rewrites: []RewriteRule{},
		Redirects: []RedirectRule{}, CommandWebhooks: []CommandWebhookRule{},
		CustomHeaders: []CustomHeader{}, MockRoutes: []MockRoute{},
		VirtualHosts: []VirtualHost{},
		ConfigFilePath: *configFlag,
	}

	if *excludeFlag != "" {
		cfg.WatchExcludeDirs = strings.Split(*excludeFlag, ",")
	}
	if *phpFlag != "" {
		cfg.FastCGI = FastCGIConfig{Enabled: true, Address: *phpFlag, Extensions: []string{".php"}}
	}

	// Carrega config.json (flags têm precedência)
	if *configFlag != "" {
		fileCfg := Config{}
		if err := loadConfig(*configFlag, &fileCfg); err != nil {
			log.Fatalf("Erro config: %v", err)
		}
		if fileCfg.ProxyRules != nil       { cfg.ProxyRules = fileCfg.ProxyRules }
		if fileCfg.Rewrites != nil         { cfg.Rewrites = fileCfg.Rewrites }
		if fileCfg.Redirects != nil        { cfg.Redirects = fileCfg.Redirects }
		if fileCfg.CommandWebhooks != nil  { cfg.CommandWebhooks = fileCfg.CommandWebhooks }
		if fileCfg.CustomHeaders != nil    { cfg.CustomHeaders = fileCfg.CustomHeaders }
		if fileCfg.MockRoutes != nil       { cfg.MockRoutes = fileCfg.MockRoutes }
		if fileCfg.VirtualHosts != nil     { cfg.VirtualHosts = fileCfg.VirtualHosts }
		if fileCfg.APICommandAllowList != nil { cfg.APICommandAllowList = fileCfg.APICommandAllowList }
		cfg.APICommandEnabled = fileCfg.APICommandEnabled
		if fileCfg.FastCGI.Enabled && *phpFlag == "" { cfg.FastCGI = fileCfg.FastCGI }
		if fileCfg.RateLimit.Enabled && !*rateLimitFlag { cfg.RateLimit = fileCfg.RateLimit }
		if fileCfg.WatchExcludeDirs != nil && *excludeFlag == "" { cfg.WatchExcludeDirs = fileCfg.WatchExcludeDirs }
		if *portFlag == 5571 && fileCfg.Port != 0       { cfg.Port = fileCfg.Port }
		if *serveDirFlag == "www" && fileCfg.ServeDir != "" { cfg.ServeDir = fileCfg.ServeDir }
		if *logFileFlag == "server.log" && fileCfg.LogFilePath != "" { cfg.LogFilePath = fileCfg.LogFilePath }
		if *apiTokenFlag == "" && fileCfg.APIToken != "" { cfg.APIToken = fileCfg.APIToken }
		if fileCfg.Custom404PagePath != "" && *page404Flag == "" { cfg.Custom404PagePath = fileCfg.Custom404PagePath }
		if fileCfg.HTTPSCertFile != "" { cfg.HTTPSCertFile = fileCfg.HTTPSCertFile }
		if fileCfg.HTTPSKeyFile != ""  { cfg.HTTPSKeyFile = fileCfg.HTTPSKeyFile }
	}

	// Config global para hot reload
	currentCfgMu.Lock()
	currentCfg = cfg
	currentCfgMu.Unlock()

	// Log em arquivo
	if cfg.LogFilePath != "" {
		f, err := os.OpenFile(cfg.LogFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Log: %v", err)
		}
		log.SetOutput(f)
	}

	if _, err := os.Stat(cfg.ServeDir); os.IsNotExist(err) {
		if err2 := os.MkdirAll(cfg.ServeDir, 0755); err2 != nil {
			log.Fatalf("Diretório '%s' não encontrado e não foi possível criar.", cfg.ServeDir)
		}
		logLine(fmt.Sprintf("Diretório '%s' criado automaticamente.", cfg.ServeDir))
	}

	injJS  := readFileContent(cfg.InjectJSPath)
	injCSS := readFileContent(cfg.InjectCSSPath)

	go handleMessages()
	go watchFiles(cfg)

	// ── Roteador ────────────────────────────────────────────────────────────────
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleConnections)

	// Dashboard
	if cfg.DashboardEnabled {
		mux.HandleFunc("/___brhttp", dashboardHandler)
		mux.HandleFunc("/___brhttp/", dashboardHandler)
		mux.HandleFunc("/___brhttp/metrics-json", metricsJSONHandler)
	}

	// Métricas Prometheus
	if cfg.MetricsEnabled {
		mux.HandleFunc("/metrics", metricsHandler)
	}

	// API
	apiMux := http.NewServeMux()

	apiMux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" { http.Error(w, "Method Not Allowed", 405); return }
		currentCfgMu.RLock()
		liveCfg := currentCfg
		currentCfgMu.RUnlock()
		clientsMu.Lock()
		cc := len(clients)
		clientsMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "running", "version": Version,
			"uptime": fmt.Sprintf("%s", time.Since(startTime).Round(time.Second)),
			"port": liveCfg.Port, "serve_dir": liveCfg.ServeDir,
			"connected_clients": cc,
			"requests": atomic.LoadInt64(&totalRequests),
			"errors":   atomic.LoadInt64(&totalErrors),
		})
	})

	apiMux.HandleFunc("/api/reload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" { http.Error(w, "Method Not Allowed", 405); return }
		msg, _ := json.Marshal(map[string]string{"type": "reload"})
		broadcast <- msg
		logLine("Live reload manual disparado via API")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	})

	apiMux.HandleFunc("/api/reload-config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" { http.Error(w, "Method Not Allowed", 405); return }
		currentCfgMu.RLock()
		cfgPath := currentCfg.ConfigFilePath
		currentCfgMu.RUnlock()
		if cfgPath == "" {
			http.Error(w, `{"error":"no config file loaded"}`, 400)
			return
		}
		newCfg := Config{ConfigFilePath: cfgPath}
		if err := loadConfig(cfgPath, &newCfg); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), 500)
			return
		}
		currentCfgMu.Lock()
		currentCfg = newCfg
		currentCfgMu.Unlock()
		logLine("Config recarregada via API")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	})

	apiMux.HandleFunc("/api/command", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" { http.Error(w, "Method Not Allowed", 405); return }
		currentCfgMu.RLock()
		liveCfg := currentCfg
		currentCfgMu.RUnlock()
		if !liveCfg.APICommandEnabled {
			http.Error(w, `{"error":"api_command_enabled is false"}`, 403)
			return
		}
		var req struct {
			Command string   `json:"command"`
			Args    []string `json:"args"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"bad request"}`, 400)
			return
		}
		if len(liveCfg.APICommandAllowList) > 0 {
			allowed := false
			for _, c := range liveCfg.APICommandAllowList {
				if c == req.Command { allowed = true; break }
			}
			if !allowed {
				http.Error(w, fmt.Sprintf(`{"error":"command '%s' not in allow list"}`, req.Command), 403)
				return
			}
		}
		go func() {
			cmd := exec.Command(req.Command, req.Args...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			logLine(fmt.Sprintf("Comando API: %s %v", req.Command, req.Args))
			if err := cmd.Run(); err != nil {
				logLine(fmt.Sprintf("Erro comando: %v", err))
			}
		}()
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	})

	mux.Handle("/api/", apiAuthMiddleware(cfg.APIToken, apiMux))

	// File server
	var fileServer http.Handler
	if cfg.DirListingEnabled {
		fileServer = http.FileServer(http.Dir(cfg.ServeDir))
	} else {
		fileServer = http.FileServer(noDirListingFS{http.Dir(cfg.ServeDir)})
	}

	// Chain de middlewares (de dentro para fora)
	handler := fileServer
	handler = customErrorPageMiddleware(cfg.Custom404PagePath, cfg.ServeDir, handler)
	handler = spaFallbackMiddleware(cfg.ServeDir, cfg.SPAFallbackEnabled, handler)
	handler = liveReloadInjector(injJS, injCSS, handler)
	handler = fastCGIMiddleware(cfg.FastCGI, cfg.ServeDir, handler)
	handler = mockRoutesMiddleware(cfg.MockRoutes, handler)
	handler = virtualHostMiddleware(cfg.VirtualHosts, cfg.ServeDir, handler)
	handler = reverseProxyMiddleware(cfg.ProxyRules, handler)
	handler = rewriteRedirectMiddleware(cfg.Rewrites, cfg.Redirects, handler)
	handler = customHeadersMiddleware(cfg.CustomHeaders, handler)
	handler = etagMiddleware(cfg.ETagEnabled, cfg.CacheModeEnabled, handler)
	handler = corsMiddleware(handler)
	if !cfg.CacheModeEnabled {
		handler = noCacheMiddleware(handler)
	}
	handler = gzipMiddleware(cfg.GzipEnabled, handler)
	handler = rateLimitMiddleware(cfg.RateLimit, handler)
	handler = loggingMiddleware(handler)
	mux.Handle("/", handler)

	// ── Hot reload via SIGHUP ────────────────────────────────────────────────────
	if cfg.ConfigFilePath != "" {
		sigHUP := make(chan os.Signal, 1)
		signal.Notify(sigHUP, syscall.SIGHUP)
		go func() {
			for range sigHUP {
				newCfg := Config{ConfigFilePath: cfg.ConfigFilePath}
				if err := loadConfig(cfg.ConfigFilePath, &newCfg); err != nil {
					logLine(fmt.Sprintf("SIGHUP config erro: %v", err))
					continue
				}
				currentCfgMu.Lock()
				currentCfg = newCfg
				currentCfgMu.Unlock()
				logLine("Config recarregada via SIGHUP")
			}
		}()
	}

	// ── server_stop via SIGINT/SIGTERM ───────────────────────────────────────────
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopCh
		logLine("Servidor encerrando...")
		fireWebhooks("server_stop", map[string]string{
			"timestamp": time.Now().Format(time.RFC3339),
			"port":      strconv.Itoa(cfg.Port),
		}, cfg)
		os.Exit(0)
	}()

	// ── Webhooks server_start ────────────────────────────────────────────────────
	fireWebhooks("server_start", map[string]string{
		"timestamp": time.Now().Format(time.RFC3339),
		"port":      strconv.Itoa(cfg.Port),
		"serve_dir": cfg.ServeDir,
	}, cfg)

	// ── Banner ───────────────────────────────────────────────────────────────────
	fmt.Println()
	fmt.Println("  ██████╗ ██████╗ ██╗  ██╗████████╗████████╗██████╗ ")
	fmt.Println("  ██╔══██╗██╔══██╗██║  ██║╚══██╔══╝╚══██╔══╝██╔══██╗")
	fmt.Println("  ██████╔╝██████╔╝███████║   ██║      ██║   ██████╔╝")
	fmt.Println("  ██╔══██╗██╔══██╗██╔══██║   ██║      ██║   ██╔═══╝ ")
	fmt.Println("  ██████╔╝██║  ██║██║  ██║   ██║      ██║   ██║     ")
	fmt.Println("  ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝      ╚═╝   ╚═╝  v" + Version)
	fmt.Println()
	fmt.Printf("  🚀  HTTP      → http://localhost:%d\n", cfg.Port)
	if cfg.HTTPSEnabled {
		fmt.Printf("  🔒  HTTPS     → https://localhost:%d\n", cfg.HTTPSPort)
	}
	if cfg.DashboardEnabled {
		fmt.Printf("  📊  Dashboard → http://localhost:%d/___brhttp\n", cfg.Port)
	}
	if cfg.MetricsEnabled {
		fmt.Printf("  📈  Métricas  → http://localhost:%d/metrics\n", cfg.Port)
	}
	if cfg.FastCGI.Enabled {
		fmt.Printf("  🐘  FastCGI   → %s (%s)\n", cfg.FastCGI.Address, strings.Join(cfg.FastCGI.Extensions, ","))
	}
	if cfg.RateLimit.Enabled {
		fmt.Printf("  🛡️   Rate Limit → %d req/min, burst %d\n", cfg.RateLimit.RequestsPerMinute, cfg.RateLimit.BurstSize)
	}
	fmt.Printf("  📁  Servindo  → %s\n", cfg.ServeDir)
	fmt.Printf("  ⚡  Live Reload: ativado\n")
	if cfg.APIToken == "" {
		fmt.Printf("  ⚠️   api_token não configurado\n")
	}
	if len(cfg.MockRoutes) > 0 {
		fmt.Printf("  🎭  Mock Routes: %d configuradas\n", len(cfg.MockRoutes))
	}
	if len(cfg.VirtualHosts) > 0 {
		fmt.Printf("  🌐  Virtual Hosts: %d configurados\n", len(cfg.VirtualHosts))
	}
	fmt.Println()

	// ── HTTPS ────────────────────────────────────────────────────────────────────
	if cfg.HTTPSEnabled {
		var tlsCfg *tls.Config
		if cfg.HTTPSCertFile != "" && cfg.HTTPSKeyFile != "" {
			cert, err := tls.LoadX509KeyPair(cfg.HTTPSCertFile, cfg.HTTPSKeyFile)
			if err != nil { log.Fatalf("TLS: %v", err) }
			tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}}
		} else {
			cert, err := generateSelfSignedCert()
			if err != nil { log.Fatalf("TLS auto-assinado: %v", err) }
			tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}}
			logLine("Certificado TLS auto-assinado gerado (localhost, válido 1 ano)")
		}
		go func() {
			srv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.HTTPSPort), Handler: mux, TLSConfig: tlsCfg}
			logLine(fmt.Sprintf("HTTPS em https://localhost:%d", cfg.HTTPSPort))
			if err := srv.ListenAndServeTLS("", ""); err != nil {
				log.Fatalf("HTTPS erro: %v", err)
			}
		}()
	}

	// ── HTTP ─────────────────────────────────────────────────────────────────────
	addr := fmt.Sprintf(":%d", cfg.Port)
	logLine(fmt.Sprintf("HTTP em http://localhost%s", addr))
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("HTTP erro: %v", err)
	}
}