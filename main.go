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

const Version = "4.0.0"

// ─── Tipos de configuração ────────────────────────────────────────────────────

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

type MockRoute struct {
	Path       string            `json:"path"`
	Method     string            `json:"method"`
	File       string            `json:"file"`
	Body       string            `json:"body"`
	StatusCode int               `json:"status_code"`
	DelayMs    int               `json:"delay_ms"`
	Headers    map[string]string `json:"headers"`
}

type VirtualHost struct {
	Host        string `json:"host"`
	ServeDir    string `json:"serve_dir"`
	SPAFallback bool   `json:"spa_fallback"`
	Runtime     string `json:"runtime"` // php, python, node, ruby, static
}

type FastCGIConfig struct {
	Enabled    bool     `json:"enabled"`
	Address    string   `json:"address"`
	Extensions []string `json:"extensions"`
	Root       string   `json:"root"`
	AutoStart  bool     `json:"auto_start"`
}

type RateLimitConfig struct {
	Enabled           bool `json:"enabled"`
	RequestsPerMinute int  `json:"requests_per_minute"`
	BurstSize         int  `json:"burst_size"`
}

// ProcessConfig — define um processo gerenciado pelo supervisor
type ProcessConfig struct {
	Name       string   `json:"name"`
	Command    string   `json:"command"`
	Args       []string `json:"args"`
	Dir        string   `json:"dir"`
	Env        []string `json:"env"`
	AutoStart  bool     `json:"auto_start"`
	AutoRestart bool    `json:"auto_restart"`
	Port       int      `json:"port"` // porta que o processo ocupa
}

// ExtraPort — porta adicional com config própria
type ExtraPort struct {
	Port    int    `json:"port"`
	Dir     string `json:"dir"`
	HTTPS   bool   `json:"https"`
	Runtime string `json:"runtime"`
}

// HostsEntry — entrada no /etc/hosts
type HostsEntry struct {
	IP     string `json:"ip"`
	Domain string `json:"domain"`
}

// DNSConfig — servidor DNS local embutido
type DNSConfig struct {
	Enabled  bool     `json:"enabled"`
	Port     int      `json:"port"`
	Domains  []string `json:"domains"` // ex: ["*.localhost", "*.dev.local"]
	Upstream string   `json:"upstream"` // ex: "8.8.8.8:53"
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
	MockRoutes             []MockRoute          `json:"mock_routes"`
	VirtualHosts           []VirtualHost        `json:"virtual_hosts"`
	FastCGI                FastCGIConfig        `json:"fastcgi"`
	RateLimit              RateLimitConfig      `json:"rate_limit"`
	ETagEnabled            bool                 `json:"etag_enabled"`
	MetricsEnabled         bool                 `json:"metrics_enabled"`
	CacheModeEnabled       bool                 `json:"cache_mode_enabled"`
	// v4.0
	Processes              []ProcessConfig      `json:"processes"`
	ExtraPorts             []ExtraPort          `json:"extra_ports"`
	HostsEntries           []HostsEntry         `json:"hosts_entries"`
	DNS                    DNSConfig            `json:"dns"`
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
	broadcast = make(chan []byte, 256)
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

	// Process supervisor
	supervisor *ProcessSupervisor
)

// ─── Rate Limiter ─────────────────────────────────────────────────────────────

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

// ─── Process Supervisor ───────────────────────────────────────────────────────

type ProcessStatus struct {
	Name    string    `json:"name"`
	Command string    `json:"command"`
	State   string    `json:"state"` // running, stopped, error
	PID     int       `json:"pid"`
	Uptime  string    `json:"uptime"`
	Port    int       `json:"port"`
	Started time.Time `json:"started"`
}

type ManagedProcess struct {
	config  ProcessConfig
	cmd     *exec.Cmd
	state   string
	started time.Time
	mu      sync.Mutex
	stopCh  chan struct{}
}

type ProcessSupervisor struct {
	processes map[string]*ManagedProcess
	mu        sync.Mutex
}

func NewProcessSupervisor() *ProcessSupervisor {
	return &ProcessSupervisor{
		processes: make(map[string]*ManagedProcess),
	}
}

func (s *ProcessSupervisor) Register(cfg ProcessConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processes[cfg.Name] = &ManagedProcess{
		config: cfg,
		state:  "stopped",
		stopCh: make(chan struct{}),
	}
}

func (s *ProcessSupervisor) Start(name string) error {
	s.mu.Lock()
	p, ok := s.processes[name]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("processo '%s' não encontrado", name)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == "running" {
		return nil
	}
	cmd := exec.Command(p.config.Command, p.config.Args...)
	if p.config.Dir != "" {
		cmd.Dir = p.config.Dir
	}
	cmd.Env = append(os.Environ(), p.config.Env...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		p.state = "error"
		logLine(fmt.Sprintf("Supervisor: erro ao iniciar '%s': %v", name, err))
		return err
	}
	p.cmd = cmd
	p.state = "running"
	p.started = time.Now()
	logLine(fmt.Sprintf("Supervisor: '%s' iniciado (PID %d)", name, cmd.Process.Pid))

	// Auto-restart
	if p.config.AutoRestart {
		go func() {
			cmd.Wait()
			p.mu.Lock()
			if p.state == "running" {
				p.state = "stopped"
				p.mu.Unlock()
				logLine(fmt.Sprintf("Supervisor: '%s' encerrou, reiniciando...", name))
				time.Sleep(2 * time.Second)
				s.Start(name)
			} else {
				p.mu.Unlock()
			}
		}()
	}
	return nil
}

func (s *ProcessSupervisor) Stop(name string) error {
	s.mu.Lock()
	p, ok := s.processes[name]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("processo '%s' não encontrado", name)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state != "running" || p.cmd == nil {
		p.state = "stopped"
		return nil
	}
	p.state = "stopped"
	if err := p.cmd.Process.Kill(); err != nil {
		return err
	}
	logLine(fmt.Sprintf("Supervisor: '%s' parado", name))
	return nil
}

func (s *ProcessSupervisor) Restart(name string) error {
	s.Stop(name)
	time.Sleep(500 * time.Millisecond)
	return s.Start(name)
}

func (s *ProcessSupervisor) Status() []ProcessStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ProcessStatus
	for _, p := range s.processes {
		p.mu.Lock()
		ps := ProcessStatus{
			Name:    p.config.Name,
			Command: p.config.Command + " " + strings.Join(p.config.Args, " "),
			State:   p.state,
			Port:    p.config.Port,
		}
		if p.cmd != nil && p.cmd.Process != nil {
			ps.PID = p.cmd.Process.Pid
		}
		if p.state == "running" {
			ps.Uptime = time.Since(p.started).Round(time.Second).String()
			ps.Started = p.started
		}
		p.mu.Unlock()
		out = append(out, ps)
	}
	return out
}

func (s *ProcessSupervisor) StopAll() {
	s.mu.Lock()
	names := make([]string, 0, len(s.processes))
	for name := range s.processes {
		names = append(names, name)
	}
	s.mu.Unlock()
	for _, name := range names {
		s.Stop(name)
	}
}

// detectRuntime detecta qual runtime está disponível no sistema
func detectRuntime(runtime string) (string, []string, bool) {
	switch runtime {
	case "php":
		// Tenta php-fpm, depois php
		for _, bin := range []string{"php-fpm8.2", "php-fpm8.1", "php-fpm8.0", "php-fpm", "php"} {
			if path, err := exec.LookPath(bin); err == nil {
				if bin == "php" {
					return path, []string{"-S", "127.0.0.1:9000"}, true
				}
				return path, []string{"-F"}, true
			}
		}
	case "python":
		for _, bin := range []string{"python3", "python"} {
			if path, err := exec.LookPath(bin); err == nil {
				return path, []string{"-m", "gunicorn", "--bind", "127.0.0.1:8000", "wsgi:app"}, true
			}
		}
	case "node":
		if path, err := exec.LookPath("node"); err == nil {
			return path, []string{"server.js"}, true
		}
	case "ruby":
		for _, bin := range []string{"bundle", "ruby"} {
			if path, err := exec.LookPath(bin); err == nil {
				if bin == "bundle" {
					return path, []string{"exec", "rails", "server", "-p", "3000"}, true
				}
				return path, []string{"app.rb"}, true
			}
		}
	}
	return "", nil, false
}

// ─── DNS local embutido ───────────────────────────────────────────────────────

func startDNSServer(cfg DNSConfig) {
	if !cfg.Enabled {
		return
	}
	port := cfg.Port
	if port == 0 {
		port = 5353 // porta alternativa para não precisar de root
	}
	upstream := cfg.Upstream
	if upstream == "" {
		upstream = "8.8.8.8:53"
	}

	addr := fmt.Sprintf(":%d", port)
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		logLine(fmt.Sprintf("DNS: erro ao iniciar em %s: %v (tente porta > 1024 ou rode como root)", addr, err))
		return
	}

	logLine(fmt.Sprintf("DNS local em UDP%s — resolve: %v", addr, cfg.Domains))

	go func() {
		defer pc.Close()
		buf := make([]byte, 512)
		for {
			n, clientAddr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			go handleDNSQuery(pc, clientAddr, buf[:n], cfg.Domains, upstream)
		}
	}()
}

func handleDNSQuery(pc net.PacketConn, addr net.Addr, query []byte, localDomains []string, upstream string) {
	// Extrai o nome do domínio da query DNS
	if len(query) < 12 {
		return
	}

	// Parse simples do nome DNS
	name := parseDNSName(query[12:])
	isLocal := false
	for _, pattern := range localDomains {
		if matchDomain(name, pattern) {
			isLocal = true
			break
		}
	}

	if isLocal {
		// Responde com 127.0.0.1
		resp := buildDNSResponse(query, "127.0.0.1")
		pc.WriteTo(resp, addr)
		logLine(fmt.Sprintf("DNS: %s → 127.0.0.1 (local)", name))
		return
	}

	// Encaminha para upstream
	upConn, err := net.DialTimeout("udp", upstream, 3*time.Second)
	if err != nil {
		return
	}
	defer upConn.Close()
	upConn.SetDeadline(time.Now().Add(3 * time.Second))
	upConn.Write(query)
	resp := make([]byte, 512)
	n, err := upConn.Read(resp)
	if err != nil {
		return
	}
	pc.WriteTo(resp[:n], addr)
}

func parseDNSName(data []byte) string {
	var name []string
	i := 0
	for i < len(data) {
		length := int(data[i])
		if length == 0 {
			break
		}
		i++
		if i+length > len(data) {
			break
		}
		name = append(name, string(data[i:i+length]))
		i += length
	}
	return strings.Join(name, ".")
}

func matchDomain(name, pattern string) bool {
	if strings.HasPrefix(pattern, "*.") {
		suffix := pattern[1:] // .localhost
		return strings.HasSuffix(name, suffix) || name == suffix[1:]
	}
	return name == pattern
}

func buildDNSResponse(query []byte, ip string) []byte {
	resp := make([]byte, len(query)+16)
	copy(resp, query)
	// Flags: response, no error
	resp[2] = 0x81
	resp[3] = 0x80
	// ANCOUNT = 1
	resp[6] = 0
	resp[7] = 1

	// Answer section
	offset := len(query)
	// Pointer to question name
	resp[offset] = 0xc0
	resp[offset+1] = 0x0c
	// Type A
	resp[offset+2] = 0
	resp[offset+3] = 1
	// Class IN
	resp[offset+4] = 0
	resp[offset+5] = 1
	// TTL = 60
	resp[offset+6] = 0
	resp[offset+7] = 0
	resp[offset+8] = 0
	resp[offset+9] = 60
	// RDLENGTH = 4
	resp[offset+10] = 0
	resp[offset+11] = 4
	// IP
	parts := strings.Split(ip, ".")
	for i, p := range parts {
		n, _ := strconv.Atoi(p)
		resp[offset+12+i] = byte(n)
	}
	return resp[:offset+16]
}

// ─── /etc/hosts manager ───────────────────────────────────────────────────────

const hostsMarker = "# brhttp managed entries"

func addHostsEntry(entry HostsEntry) error {
	hostsPath := "/etc/hosts"
	if os.Getenv("GOOS") == "windows" {
		hostsPath = `C:\Windows\System32\drivers\etc\hosts`
	}

	content, err := os.ReadFile(hostsPath)
	if err != nil {
		return fmt.Errorf("erro lendo /etc/hosts: %w", err)
	}

	line := entry.IP + "\t" + entry.Domain
	if strings.Contains(string(content), line) {
		return nil // já existe
	}

	newContent := string(content)
	if !strings.Contains(newContent, hostsMarker) {
		newContent += "\n" + hostsMarker + "\n"
	}
	newContent += line + "\n"

	if err := os.WriteFile(hostsPath, []byte(newContent), 0644); err != nil {
		return fmt.Errorf("erro escrevendo /etc/hosts (tente com sudo): %w", err)
	}
	logLine(fmt.Sprintf("Hosts: adicionado %s → %s", entry.Domain, entry.IP))
	return nil
}

func removeHostsEntry(domain string) error {
	hostsPath := "/etc/hosts"
	content, err := os.ReadFile(hostsPath)
	if err != nil {
		return err
	}
	lines := strings.Split(string(content), "\n")
	var kept []string
	for _, line := range lines {
		if !strings.Contains(line, "\t"+domain) && !strings.Contains(line, " "+domain) {
			kept = append(kept, line)
		}
	}
	return os.WriteFile(hostsPath, []byte(strings.Join(kept, "\n")), 0644)
}

func listHostsEntries() []HostsEntry {
	content, err := os.ReadFile("/etc/hosts")
	if err != nil {
		return nil
	}
	var entries []HostsEntry
	inManaged := false
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if line == hostsMarker {
			inManaged = true
			continue
		}
		if inManaged && line != "" && !strings.HasPrefix(line, "#") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				entries = append(entries, HostsEntry{IP: parts[0], Domain: parts[1]})
			}
		}
	}
	return entries
}

// ─── WebSocket ────────────────────────────────────────────────────────────────

type Client struct {
	conn *websocket.Conn
	send chan []byte
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
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

// ─── FastCGI ──────────────────────────────────────────────────────────────────

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
		root := serveDir
		if cfg.Root != "" {
			root = cfg.Root
		}
		if !isFCGI {
			fp := filepath.Join(root, r.URL.Path)
			if info, err := os.Stat(fp); err == nil && info.IsDir() {
				if _, err2 := os.Stat(filepath.Join(fp, "index.php")); err2 == nil {
					isFCGI = true
				}
			}
		}
		if !isFCGI {
			next.ServeHTTP(w, r)
			return
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
			http.Error(w, "<h2>PHP-FPM indisponível</h2><p>Inicie com: <code>php-fpm</code> ou habilite auto_start no config.</p><p>Endereço: <code>"+addr+"</code></p>", 502)
			return
		}
		defer conn.Close()
		params := map[string]string{
			"SERVER_SOFTWARE": "brhttp/" + Version,
			"SERVER_NAME": r.Host, "GATEWAY_INTERFACE": "CGI/1.1",
			"SERVER_PROTOCOL": r.Proto, "REQUEST_METHOD": r.Method,
			"SCRIPT_FILENAME": filePath, "SCRIPT_NAME": r.URL.Path,
			"PATH_INFO": r.URL.Path, "QUERY_STRING": r.URL.RawQuery,
			"REQUEST_URI": r.RequestURI, "DOCUMENT_ROOT": root,
			"REMOTE_ADDR": getClientIP(r), "CONTENT_TYPE": r.Header.Get("Content-Type"),
			"CONTENT_LENGTH": r.Header.Get("Content-Length"),
			"HTTP_HOST": r.Host, "HTTP_USER_AGENT": r.Header.Get("User-Agent"),
			"HTTP_ACCEPT": r.Header.Get("Accept"), "HTTP_COOKIE": r.Header.Get("Cookie"),
			"REDIRECT_STATUS": "200",
		}
		if r.TLS != nil {
			params["HTTPS"] = "on"
		}
		if err := sendFCGIRequest(conn, params, r, w); err != nil {
			logLine(fmt.Sprintf("FastCGI erro: %v", err))
		}
	})
}

func sendFCGIRequest(conn net.Conn, params map[string]string, r *http.Request, w http.ResponseWriter) error {
	const (
		fcgiBeginRequest = 1
		fcgiParams       = 4
		fcgiStdin        = 5
		fcgiStdout       = 6
		fcgiStderr       = 7
		fcgiEndRequest   = 3
		fcgiVersion      = 1
		fcgiResponder    = 1
	)
	reqID := uint16(1)
	write := func(recType uint8, data []byte) error {
		pad := (8 - len(data)%8) % 8
		hdr := []byte{fcgiVersion, recType, uint8(reqID >> 8), uint8(reqID), uint8(len(data) >> 8), uint8(len(data)), uint8(pad), 0}
		conn.Write(hdr)
		conn.Write(data)
		if pad > 0 {
			conn.Write(make([]byte, pad))
		}
		return nil
	}
	write(fcgiBeginRequest, []byte{0, fcgiResponder, 0, 0, 0, 0, 0, 0})
	var pb bytes.Buffer
	for k, v := range params {
		kl, vl := len(k), len(v)
		if kl > 127 {
			pb.Write([]byte{byte(kl>>24) | 0x80, byte(kl >> 16), byte(kl >> 8), byte(kl)})
		} else {
			pb.WriteByte(byte(kl))
		}
		if vl > 127 {
			pb.Write([]byte{byte(vl>>24) | 0x80, byte(vl >> 16), byte(vl >> 8), byte(vl)})
		} else {
			pb.WriteByte(byte(vl))
		}
		pb.WriteString(k)
		pb.WriteString(v)
	}
	write(fcgiParams, pb.Bytes())
	write(fcgiParams, []byte{})
	if r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		if len(body) > 0 {
			write(fcgiStdin, body)
		}
	}
	write(fcgiStdin, []byte{})
	var respBuf bytes.Buffer
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(conn, hdr); err != nil {
			break
		}
		recType := hdr[1]
		cl := int(hdr[4])<<8 | int(hdr[5])
		pl := int(hdr[6])
		content := make([]byte, cl)
		if cl > 0 {
			io.ReadFull(conn, content)
		}
		if pl > 0 {
			io.ReadFull(conn, make([]byte, pl))
		}
		switch recType {
		case fcgiStdout:
			if cl > 0 {
				respBuf.Write(content)
			}
		case fcgiStderr:
			if cl > 0 {
				logLine("PHP stderr: " + string(content))
			}
		case fcgiEndRequest:
			goto done
		}
	}
done:
	data := respBuf.Bytes()
	he := bytes.Index(data, []byte("\r\n\r\n"))
	sep := 4
	if he == -1 {
		he = bytes.Index(data, []byte("\n\n"))
		sep = 2
	}
	if he == -1 {
		w.Write(data)
		return nil
	}
	headerSection := string(data[:he])
	body := data[he+sep:]
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

// ─── Métricas ─────────────────────────────────────────────────────────────────

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	clientsMu.Lock()
	cc := len(clients)
	clientsMu.Unlock()
	reqs := atomic.LoadInt64(&totalRequests)
	errs := atomic.LoadInt64(&totalErrors)
	byt := atomic.LoadInt64(&totalBytes)
	uptime := time.Since(startTime).Seconds()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintf(w, "# HELP brhttp_requests_total Total requisições\n# TYPE brhttp_requests_total counter\nbrhttp_requests_total %d\n\n", reqs)
	fmt.Fprintf(w, "# HELP brhttp_errors_total Total erros 4xx/5xx\n# TYPE brhttp_errors_total counter\nbrhttp_errors_total %d\n\n", errs)
	fmt.Fprintf(w, "# HELP brhttp_bytes_total Total bytes\n# TYPE brhttp_bytes_total counter\nbrhttp_bytes_total %d\n\n", byt)
	fmt.Fprintf(w, "# HELP brhttp_websocket_clients Clientes WS\n# TYPE brhttp_websocket_clients gauge\nbrhttp_websocket_clients %d\n\n", cc)
	fmt.Fprintf(w, "# HELP brhttp_uptime_seconds Uptime\n# TYPE brhttp_uptime_seconds gauge\nbrhttp_uptime_seconds %.2f\n", uptime)
}

func metricsJSONHandler(w http.ResponseWriter, r *http.Request) {
	clientsMu.Lock()
	cc := len(clients)
	clientsMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"requests": atomic.LoadInt64(&totalRequests),
		"errors":   atomic.LoadInt64(&totalErrors),
		"bytes":    atomic.LoadInt64(&totalBytes),
		"ws_clients": cc,
		"uptime":   fmt.Sprintf("%s", time.Since(startTime).Round(time.Second)),
	})
}

// ─── Dashboard v4 ─────────────────────────────────────────────────────────────

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

	// Status dos processos
	var procRows string
	if supervisor != nil {
		for _, ps := range supervisor.Status() {
			stateColor := "#f85149"
			if ps.State == "running" {
				stateColor = "#3fb950"
			}
			port := ""
			if ps.Port > 0 {
				port = strconv.Itoa(ps.Port)
			}
			procRows += `<tr>
				<td>` + ps.Name + `</td>
				<td style="color:` + stateColor + `">` + ps.State + `</td>
				<td>` + strconv.Itoa(ps.PID) + `</td>
				<td>` + port + `</td>
				<td>` + ps.Uptime + `</td>
				<td>
					<button onclick="procAction('start','` + ps.Name + `')" style="font-size:10px;padding:2px 6px;cursor:pointer;background:transparent;border:1px solid #3fb950;color:#3fb950;border-radius:4px">start</button>
					<button onclick="procAction('stop','` + ps.Name + `')" style="font-size:10px;padding:2px 6px;cursor:pointer;background:transparent;border:1px solid #f85149;color:#f85149;border-radius:4px">stop</button>
					<button onclick="procAction('restart','` + ps.Name + `')" style="font-size:10px;padding:2px 6px;cursor:pointer;background:transparent;border:1px solid #58a6ff;color:#58a6ff;border-radius:4px">restart</button>
				</td>
			</tr>`
		}
	}
	if procRows == "" {
		procRows = `<tr><td colspan="6" style="color:#8b949e">Nenhum processo gerenciado</td></tr>`
	}

	// Proxy rows
	var proxyRows string
	for _, p := range cfg.ProxyRules {
		ws := ""
		if p.WebSocketEnabled {
			ws = " [WS]"
		}
		proxyRows += `<tr><td>` + p.Path + `</td><td>` + p.Target + ws + `</td>
			<td><button onclick="delProxy('` + p.Path + `')" style="font-size:10px;padding:1px 6px;cursor:pointer;background:transparent;border:1px solid #f85149;color:#f85149;border-radius:3px">×</button></td></tr>`
	}
	if proxyRows == "" {
		proxyRows = `<tr><td colspan="3" style="color:#8b949e">Nenhuma regra</td></tr>`
	}

	// Hosts entries
	var hostsRows string
	for _, h := range listHostsEntries() {
		hostsRows += `<tr><td>` + h.Domain + `</td><td>` + h.IP + `</td>
			<td><button onclick="delHost('` + h.Domain + `')" style="font-size:10px;padding:1px 6px;cursor:pointer;background:transparent;border:1px solid #f85149;color:#f85149;border-radius:3px">×</button></td></tr>`
	}
	if hostsRows == "" {
		hostsRows = `<tr><td colspan="3" style="color:#8b949e">Nenhuma entrada gerenciada</td></tr>`
	}

	badge := func(b bool, label string) string {
		cls := ""; txt := "OFF"
		if b { cls = " on"; txt = "ON" }
		return `<span class="badge` + cls + `" onclick="toggleModule('` + strings.ToLower(strings.ReplaceAll(label, " ", "_")) + `',` + strconv.FormatBool(!b) + `)">` + label + ` ` + txt + `</span>`
	}

	httpsLink := ""
	if cfg.HTTPSEnabled {
		httpsLink = ` · <a href="https://localhost:` + strconv.Itoa(cfg.HTTPSPort) + `" style="color:#3fb950" target="_blank">https:` + strconv.Itoa(cfg.HTTPSPort) + `</a>`
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
header{background:var(--s);border-bottom:1px solid var(--b);padding:12px 20px;display:flex;align-items:center;gap:10px;position:sticky;top:0;z-index:10}
h1{font-size:1rem;font-weight:700;color:var(--a)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--g);animation:pulse 2s infinite;flex-shrink:0}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px;padding:14px}
.card{background:var(--s);border:1px solid var(--b);border-radius:8px;padding:12px}
.label{font-size:10px;color:var(--m);text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px}
.val{font-size:1.5rem;font-weight:700}
.sub{font-size:11px;color:var(--m);margin-top:2px}
section{padding:0 14px 14px}
section h2{font-size:10px;color:var(--m);text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;padding-top:2px}
.log-box{background:var(--s);border:1px solid var(--b);border-radius:8px;padding:10px;height:240px;overflow-y:auto;font-size:11px;line-height:1.5}
.log-line{color:var(--m);border-bottom:1px solid #21262d;padding:1px 0}
.log-line:last-child{color:var(--t)}
table{width:100%;border-collapse:collapse}
th{text-align:left;color:var(--m);font-weight:500;padding:5px 8px;border-bottom:1px solid var(--b);font-size:11px}
td{padding:5px 8px;border-bottom:1px solid #21262d;font-size:12px}
.actions{padding:0 14px 14px;display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.btn{background:transparent;color:var(--a);border:1px solid var(--a);border-radius:5px;padding:4px 10px;font-size:11px;cursor:pointer}
.btn:hover{background:var(--a);color:#000}
.btn.g{color:var(--g);border-color:var(--g)}.btn.g:hover{background:var(--g);color:#000}
.btn.r{color:var(--r);border-color:var(--r)}.btn.r:hover{background:var(--r);color:#fff}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;margin:2px;border:1px solid var(--b);color:var(--m);cursor:pointer;user-select:none}
.badge:hover{border-color:var(--a)}
.badge.on{border-color:#3fb95055;background:#3fb95015;color:var(--g)}
#flash{font-size:11px;color:var(--g);display:none;margin-left:6px}
.two{display:grid;grid-template-columns:1fr 1fr;gap:10px;padding:0 14px 14px}
@media(max-width:600px){.two{grid-template-columns:1fr}}
input[type=text]{background:var(--s);border:1px solid var(--b);color:var(--t);padding:4px 8px;border-radius:4px;font-size:12px;width:100%}
.form-row{display:flex;gap:6px;margin-bottom:6px}
.form-row input{flex:1}
</style>
</head>
<body>
<header>
  <div class="dot"></div>
  <h1>brhttp v` + Version + `</h1>
  <span style="color:var(--m);margin-left:auto;font-size:12px">
    <a href="http://localhost:` + strconv.Itoa(cfg.Port) + `" style="color:var(--a)" target="_blank">:` + strconv.Itoa(cfg.Port) + `</a>` + httpsLink + `
  </span>
</header>

<div class="grid">
  <div class="card"><div class="label">Status</div><div class="val" style="font-size:1rem;color:var(--g)">● Online</div><div class="sub" id="up">` + uptime + `</div></div>
  <div class="card"><div class="label">Requisições</div><div class="val" id="req">` + strconv.FormatInt(reqs, 10) + `</div><div class="sub">total</div></div>
  <div class="card"><div class="label">Erros</div><div class="val" id="err" style="color:var(--r)">` + strconv.FormatInt(errs, 10) + `</div><div class="sub">4xx/5xx</div></div>
  <div class="card"><div class="label">WebSocket</div><div class="val" id="ws">` + strconv.Itoa(cc) + `</div><div class="sub">clientes</div></div>
</div>

<section>
  <h2>Módulos — clique para ligar/desligar</h2>
  ` + badge(cfg.GzipEnabled, "Gzip") +
		badge(cfg.BrotliEnabled, "Brotli") +
		badge(cfg.SPAFallbackEnabled, "SPA") +
		badge(cfg.FastCGI.Enabled, "FastCGI/PHP") +
		badge(cfg.RateLimit.Enabled, "Rate Limit") +
		badge(cfg.ETagEnabled, "ETag") +
		badge(cfg.CacheModeEnabled, "Cache") +
		badge(cfg.MetricsEnabled, "Métricas") +
		badge(cfg.HTTPSEnabled, "HTTPS") +
		badge(cfg.DNS.Enabled, "DNS local") + `
</section>

<div class="actions">
  <button class="btn" onclick="act('reload')">⚡ Live Reload</button>
  <button class="btn" onclick="act('reload-config')">🔄 Config</button>
  <a href="/metrics" target="_blank"><button class="btn">📊 Metrics</button></a>
  <span id="flash">✓</span>
</div>

<section>
  <h2>Processos gerenciados (` + strconv.Itoa(len(supervisor.Status())) + `)</h2>
  <table>
    <tr><th>Nome</th><th>Estado</th><th>PID</th><th>Porta</th><th>Uptime</th><th>Ações</th></tr>
    ` + procRows + `
  </table>
  <div style="margin-top:8px;display:flex;gap:6px">
    <button class="btn g" onclick="autoDetect()">+ Auto-detectar PHP/Python/Node</button>
  </div>
</section>

<div class="two">
  <section style="padding:0">
    <h2 style="margin-bottom:8px">Proxy rules (` + strconv.Itoa(len(cfg.ProxyRules)) + `)</h2>
    <table><tr><th>Path</th><th>Target</th><th></th></tr>` + proxyRows + `</table>
    <div class="form-row" style="margin-top:8px">
      <input type="text" id="proxy-path" placeholder="/api/">
      <input type="text" id="proxy-target" placeholder="http://localhost:3000">
      <button class="btn g" onclick="addProxy()">+</button>
    </div>
  </section>

  <section style="padding:0">
    <h2 style="margin-bottom:8px">DNS / /etc/hosts</h2>
    <table><tr><th>Domínio</th><th>IP</th><th></th></tr>` + hostsRows + `</table>
    <div class="form-row" style="margin-top:8px">
      <input type="text" id="host-domain" placeholder="meusite.local">
      <input type="text" id="host-ip" placeholder="127.0.0.1" style="max-width:120px">
      <button class="btn g" onclick="addHost()">+</button>
    </div>
  </section>
</div>

<section>
  <h2>Log em tempo real</h2>
  <div class="log-box" id="logbox"></div>
</section>

<script>
var logs = ` + string(logsJSON) + `;
var box = document.getElementById('logbox');
var TOKEN = '` + cfg.APIToken + `';
function esc(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function render(){box.innerHTML=logs.map(function(l){return '<div class="log-line">'+esc(l)+'</div>'}).join('');box.scrollTop=box.scrollHeight}
render();

var ws = new WebSocket('ws://'+location.host+'/ws');
ws.onmessage = function(e){
  var m = JSON.parse(e.data);
  if(m.type==='log'){logs.push(m.line);if(logs.length>500)logs=logs.slice(-500);render();}
};
ws.onclose = function(){setTimeout(function(){location.reload()},2000)};

function flash(msg){var f=document.getElementById('flash');f.textContent=msg||'✓';f.style.display='inline';setTimeout(function(){f.style.display='none'},2000)}
function api(path,body){
  return fetch(path,{method:'POST',headers:{'Authorization':'Bearer '+TOKEN,'Content-Type':'application/json'},body:body?JSON.stringify(body):undefined});
}
function act(ep){api('/api/'+ep).then(function(r){if(r.ok)flash()})}

function toggleModule(mod, val){
  api('/api/set',{module:mod,value:val}).then(function(r){if(r.ok){flash('✓ '+mod+' '+(val?'ON':'OFF'));setTimeout(function(){location.reload()},500)}});
}

function procAction(action, name){
  api('/api/process/'+action,{name:name}).then(function(r){if(r.ok){flash('✓ '+name+' '+action);setTimeout(function(){location.reload()},1000)}});
}

function autoDetect(){
  api('/api/process/autodetect').then(function(r){if(r.ok){flash('✓ detectado!');setTimeout(function(){location.reload()},1000)}});
}

function addProxy(){
  var path = document.getElementById('proxy-path').value;
  var target = document.getElementById('proxy-target').value;
  if(!path||!target)return;
  api('/api/config/proxy/add',{path:path,target:target}).then(function(r){if(r.ok){flash('✓ proxy adicionado');setTimeout(function(){location.reload()},500)}});
}

function delProxy(path){
  api('/api/config/proxy/delete',{path:path}).then(function(r){if(r.ok){flash('✓ removido');setTimeout(function(){location.reload()},500)}});
}

function addHost(){
  var domain = document.getElementById('host-domain').value;
  var ip = document.getElementById('host-ip').value || '127.0.0.1';
  if(!domain)return;
  api('/api/hosts/add',{domain:domain,ip:ip}).then(function(r){r.text().then(function(t){flash(r.ok?'✓ adicionado':'Erro: '+t);if(r.ok)setTimeout(function(){location.reload()},500)})});
}

function delHost(domain){
  api('/api/hosts/delete',{domain:domain}).then(function(r){if(r.ok){flash('✓ removido');setTimeout(function(){location.reload()},500)}});
}

setInterval(function(){
  fetch('/___brhttp/metrics-json').then(function(r){return r.json()}).then(function(d){
    document.getElementById('req').textContent=d.requests;
    document.getElementById('err').textContent=d.errors;
    document.getElementById('ws').textContent=d.ws_clients;
    document.getElementById('up').textContent=d.uptime;
  });
},3000);
</script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(html))
}

// ─── Middlewares ──────────────────────────────────────────────────────────────

type statusRecorder struct {
	http.ResponseWriter
	status int
	size   int
}

func (r *statusRecorder) WriteHeader(code int) { r.status = code; r.ResponseWriter.WriteHeader(code) }
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
			w.WriteHeader(200)
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

func newRR(w http.ResponseWriter) *responseRecorder {
	return &responseRecorder{ResponseWriter: w, StatusCode: 200, Body: new(bytes.Buffer), Headers: make(http.Header)}
}

func (r *responseRecorder) Header() http.Header          { return r.Headers }
func (r *responseRecorder) WriteHeader(code int)          { r.StatusCode = code }
func (r *responseRecorder) Write(b []byte) (int, error)   { return r.Body.Write(b) }
func (r *responseRecorder) CopyTo(w http.ResponseWriter) {
	for k, v := range r.Headers { w.Header()[k] = v }
	w.WriteHeader(r.StatusCode)
	w.Write(r.Body.Bytes())
}

func liveReloadInjector(injJS, injCSS string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws" || r.Method != http.MethodGet {
			next.ServeHTTP(w, r)
			return
		}
		rec := newRR(w)
		next.ServeHTTP(rec, r)
		if strings.Contains(rec.Headers.Get("Content-Type"), "text/html") && rec.StatusCode == 200 {
			body := rec.Body.Bytes()
			script := `<script>(function(){var ws=new WebSocket("ws://` + r.Host + `/ws");ws.onmessage=function(e){var m=JSON.parse(e.data);if(m.type==="reload"){location.reload();}else if(m.type==="css-update"){var l=document.querySelector('link[href*="'+m.path+'"]');if(l){l.href=m.path+'?v='+Date.now();}else{location.reload();}}else if(m.type==="js-update"){var s=document.querySelector('script[src*="'+m.path+'"]');if(s){var n=document.createElement('script');n.src=m.path+'?v='+Date.now();s.parentNode.replaceChild(n,s);}else{location.reload();}}};ws.onclose=function(){setTimeout(function(){location.reload()},1500)};})();</script>`
			var inj bytes.Buffer
			if injCSS != "" {
				inj.WriteString("<style>" + injCSS + "</style>")
			}
			if injJS != "" {
				inj.WriteString("<script>" + injJS + "</script>")
			}
			if idx := bytes.LastIndex(body, []byte("</head>")); idx != -1 {
				body = bytes.Join([][]byte{body[:idx], inj.Bytes(), body[idx:]}, nil)
			}
			if idx := bytes.LastIndex(body, []byte("</body>")); idx != -1 {
				body = bytes.Join([][]byte{body[:idx], []byte(script), body[idx:]}, nil)
			} else {
				body = append(body, []byte(script)...)
			}
			for k, v := range rec.Headers { w.Header()[k] = v }
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
		rec := newRR(w)
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

func customErrorPageMiddleware(page404, serveDir string, next http.Handler) http.Handler {
	if page404 == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := newRR(w)
		next.ServeHTTP(rec, r)
		if rec.StatusCode == 404 {
			if full := filepath.Join(serveDir, page404); func() bool { _, e := os.Stat(full); return e == nil }() {
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

func rateLimitMiddleware(cfg RateLimitConfig, next http.Handler) http.Handler {
	if !cfg.Enabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !checkRateLimit(getClientIP(r), cfg) {
			w.Header().Set("Retry-After", "60")
			http.Error(w, `{"error":"too many requests"}`, 429)
			atomic.AddInt64(&totalErrors, 1)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func mockRoutesMiddleware(routes []MockRoute, next http.Handler) http.Handler {
	if len(routes) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, route := range routes {
			if r.URL.Path != route.Path {
				continue
			}
			m := route.Method
			if m == "" {
				m = "GET"
			}
			if !strings.EqualFold(r.Method, m) {
				continue
			}
			if route.DelayMs > 0 {
				time.Sleep(time.Duration(route.DelayMs) * time.Millisecond)
			}
			for k, v := range route.Headers {
				w.Header().Set(k, v)
			}
			code := route.StatusCode
			if code == 0 {
				code = 200
			}
			var body []byte
			if route.File != "" {
				b, err := os.ReadFile(route.File)
				if err != nil {
					http.Error(w, "Mock file not found", 500)
					return
				}
				body = b
				if w.Header().Get("Content-Type") == "" {
					if strings.HasSuffix(route.File, ".json") {
						w.Header().Set("Content-Type", "application/json")
					}
				}
			} else {
				body = []byte(route.Body)
				if w.Header().Get("Content-Type") == "" && (strings.HasPrefix(strings.TrimSpace(route.Body), "{") || strings.HasPrefix(strings.TrimSpace(route.Body), "[")) {
					w.Header().Set("Content-Type", "application/json")
				}
			}
			w.WriteHeader(code)
			w.Write(body)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func virtualHostMiddleware(vhosts []VirtualHost, next http.Handler) http.Handler {
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

func reverseProxyMiddleware(rules []ProxyRule, next http.Handler) http.Handler {
	if len(rules) == 0 {
		return next
	}
	type entry struct {
		proxy *httputil.ReverseProxy
		path  string
	}
	var entries []entry
	for _, rule := range rules {
		targetURL, err := url.Parse(rule.Target)
		if err != nil {
			continue
		}
		p := httputil.NewSingleHostReverseProxy(targetURL)
		p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			http.Error(w, "Bad Gateway: "+err.Error(), 502)
		}
		orig := p.Director
		cp := rule.Path
		p.Director = func(req *http.Request) {
			orig(req)
			req.URL.Path = strings.TrimPrefix(req.URL.Path, cp)
			if req.URL.Path == "" {
				req.URL.Path = "/"
			}
		}
		entries = append(entries, entry{proxy: p, path: rule.Path})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, e := range entries {
			if strings.HasPrefix(r.URL.Path, e.path) {
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
			next.ServeHTTP(w, r)
			return
		}
		parts := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
		if len(parts) != 2 || parts[0] != "Bearer" || parts[1] != token {
			http.Error(w, `{"error":"unauthorized"}`, 401)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ─── HTTPS ────────────────────────────────────────────────────────────────────

func generateSelfSignedCert() (tls.Certificate, error) {
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{Organization: []string{"brhttp"}, CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	privDER, _ := x509.MarshalECPrivateKey(priv)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})
	return tls.X509KeyPair(certPEM, keyPEM)
}

// ─── Webhooks ─────────────────────────────────────────────────────────────────

func executeCommandWebhook(rule CommandWebhookRule, details map[string]string) {
	args := make([]string, len(rule.Args))
	for i, a := range rule.Args {
		for k, v := range details { a = strings.ReplaceAll(a, "{{"+k+"}}", v) }
		args[i] = a
	}
	cmd := exec.Command(rule.Command, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		logLine(fmt.Sprintf("Webhook erro: %v", err))
	}
}

func fireWebhooks(event string, details map[string]string, cfg Config) {
	for _, rule := range cfg.CommandWebhooks {
		if rule.Event != event { continue }
		match := rule.Path == ""
		if !match {
			if fp, ok := details["rel_path"]; ok {
				match = strings.HasPrefix(fp, rule.Path) || strings.Contains(fp, rule.Path)
			}
		}
		if match { go executeCommandWebhook(rule, details) }
	}
}

func sendNotificationWebhook(targetURL string, payload map[string]string) {
	if targetURL == "" { return }
	b, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", targetURL, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Do(req)
	if err != nil { logLine("Webhook notif erro: " + err.Error()); return }
	defer resp.Body.Close()
}

// ─── File Watcher ─────────────────────────────────────────────────────────────

func watchFiles(cfg Config) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil { log.Fatalf("Watcher: %v", err) }
	defer watcher.Close()

	debounce := time.Duration(cfg.WatchDebounceMs) * time.Millisecond
	var timer *time.Timer

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok { return }
				base := filepath.Base(event.Name)
				if strings.HasPrefix(base, ".") || strings.HasSuffix(event.Name, "~") || strings.HasSuffix(event.Name, ".tmp") { continue }
				if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove) == 0 { continue }
				if timer != nil { timer.Stop() }
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
					case ".css": msgType = "css-update"
					case ".js":  msgType = "js-update"
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
				if !ok { return }
				logLine(fmt.Sprintf("Watcher erro: %v", err))
			}
		}
	}()

	_ = filepath.Walk(cfg.ServeDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || !info.IsDir() { return nil }
		for _, excl := range cfg.WatchExcludeDirs {
			abs, _ := filepath.Abs(filepath.Join(cfg.ServeDir, excl))
			absP, _ := filepath.Abs(path)
			if strings.HasPrefix(absP, abs) { return filepath.SkipDir }
		}
		watcher.Add(path)
		return nil
	})
	select {}
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func readFileContent(p string) string {
	if p == "" { return "" }
	b, err := os.ReadFile(p)
	if err != nil { logLine("Aviso: não leu '" + p + "': " + err.Error()); return "" }
	return string(b)
}

func loadConfig(filePath string, cfg *Config) error {
	if filePath == "" { return nil }
	data, err := os.ReadFile(filePath)
	if err != nil { return fmt.Errorf("erro lendo '%s': %w", filePath, err) }
	return json.Unmarshal(data, cfg)
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	configFlag    := flag.String("config", "", "Caminho para config.json")
	portFlag      := flag.Int("port", 5571, "Porta HTTP")
	serveDirFlag  := flag.String("dir", "www", "Diretório a servir")
	injectJSFlag  := flag.String("inject-js", "", "Arquivo JS a injetar")
	injectCSSFlag := flag.String("inject-css", "", "Arquivo CSS a injetar")
	spaFlag       := flag.Bool("spa-fallback", false, "SPA fallback")
	dirListFlag   := flag.Bool("enable-dir-listing", false, "Listagem de diretórios")
	gzipFlag      := flag.Bool("enable-gzip", false, "Gzip")
	page404Flag   := flag.String("404-page", "", "Página 404 customizada")
	debounceFlag  := flag.Int("watch-debounce-ms", 100, "Debounce watcher (ms)")
	excludeFlag   := flag.String("watch-exclude-dirs", "", "Dirs excluídos do watcher")
	logFileFlag   := flag.String("log-file", "", "Arquivo de log (vazio = stdout)")
	apiTokenFlag  := flag.String("api-token", "", "Token da API")
	httpsFlag     := flag.Bool("https", false, "Habilita HTTPS auto-assinado")
	httpsPortFlag := flag.Int("https-port", 5572, "Porta HTTPS")
	dashboardFlag := flag.Bool("dashboard", true, "Dashboard em /___brhttp")
	metricsFlag   := flag.Bool("metrics", false, "Métricas Prometheus em /metrics")
	phpFlag       := flag.String("php", "", "Endereço php-fpm (ex: 127.0.0.1:9000)")
	phpAutoFlag   := flag.Bool("php-auto", false, "Auto-start PHP embutido")
	pythonFlag    := flag.String("python", "", "Endereço WSGI Python (ex: 127.0.0.1:8000)")
	nodeFlag      := flag.String("node", "", "Diretório app Node.js para auto-start")
	dnsFlag       := flag.Bool("dns", false, "DNS local em UDP:5353")
	flag.Parse()

	cfg := Config{
		Port: *portFlag, ServeDir: *serveDirFlag,
		InjectJSPath: *injectJSFlag, InjectCSSPath: *injectCSSFlag,
		SPAFallbackEnabled: *spaFlag, DirListingEnabled: *dirListFlag,
		GzipEnabled: *gzipFlag, Custom404PagePath: *page404Flag,
		WatchDebounceMs: *debounceFlag, LogFilePath: *logFileFlag,
		APIToken: *apiTokenFlag, HTTPSEnabled: *httpsFlag, HTTPSPort: *httpsPortFlag,
		DashboardEnabled: *dashboardFlag, MetricsEnabled: *metricsFlag,
		DNS: DNSConfig{Enabled: *dnsFlag, Port: 5353},
		ProxyRules: []ProxyRule{}, Rewrites: []RewriteRule{},
		Redirects: []RedirectRule{}, CommandWebhooks: []CommandWebhookRule{},
		CustomHeaders: []CustomHeader{}, MockRoutes: []MockRoute{},
		VirtualHosts: []VirtualHost{}, Processes: []ProcessConfig{},
		RateLimit: RateLimitConfig{RequestsPerMinute: 120, BurstSize: 20},
		ConfigFilePath: *configFlag,
	}

	if *excludeFlag != "" {
		cfg.WatchExcludeDirs = strings.Split(*excludeFlag, ",")
	}
	if *phpFlag != "" {
		cfg.FastCGI = FastCGIConfig{Enabled: true, Address: *phpFlag, Extensions: []string{".php"}}
	}

	// Carrega config.json
	if *configFlag != "" {
		fileCfg := Config{}
		if err := loadConfig(*configFlag, &fileCfg); err != nil {
			log.Fatalf("Erro config: %v", err)
		}
		if fileCfg.ProxyRules != nil      { cfg.ProxyRules = fileCfg.ProxyRules }
		if fileCfg.Rewrites != nil        { cfg.Rewrites = fileCfg.Rewrites }
		if fileCfg.Redirects != nil       { cfg.Redirects = fileCfg.Redirects }
		if fileCfg.CommandWebhooks != nil { cfg.CommandWebhooks = fileCfg.CommandWebhooks }
		if fileCfg.CustomHeaders != nil   { cfg.CustomHeaders = fileCfg.CustomHeaders }
		if fileCfg.MockRoutes != nil      { cfg.MockRoutes = fileCfg.MockRoutes }
		if fileCfg.VirtualHosts != nil    { cfg.VirtualHosts = fileCfg.VirtualHosts }
		if fileCfg.Processes != nil       { cfg.Processes = fileCfg.Processes }
		if fileCfg.ExtraPorts != nil      { cfg.ExtraPorts = fileCfg.ExtraPorts }
		if fileCfg.HostsEntries != nil    { cfg.HostsEntries = fileCfg.HostsEntries }
		if fileCfg.FastCGI.Enabled && *phpFlag == "" { cfg.FastCGI = fileCfg.FastCGI }
		if fileCfg.RateLimit.Enabled      { cfg.RateLimit = fileCfg.RateLimit }
		if fileCfg.DNS.Enabled            { cfg.DNS = fileCfg.DNS }
		if fileCfg.WatchExcludeDirs != nil && *excludeFlag == "" { cfg.WatchExcludeDirs = fileCfg.WatchExcludeDirs }
		if *portFlag == 5571 && fileCfg.Port != 0       { cfg.Port = fileCfg.Port }
		if *serveDirFlag == "www" && fileCfg.ServeDir != "" { cfg.ServeDir = fileCfg.ServeDir }
		if *logFileFlag == "" && fileCfg.LogFilePath != "" { cfg.LogFilePath = fileCfg.LogFilePath }
		if *apiTokenFlag == "" && fileCfg.APIToken != "" { cfg.APIToken = fileCfg.APIToken }
		if fileCfg.Custom404PagePath != "" && *page404Flag == "" { cfg.Custom404PagePath = fileCfg.Custom404PagePath }
		if fileCfg.HTTPSCertFile != "" { cfg.HTTPSCertFile = fileCfg.HTTPSCertFile }
		if fileCfg.HTTPSKeyFile != ""  { cfg.HTTPSKeyFile = fileCfg.HTTPSKeyFile }
		if fileCfg.BrotliEnabled { cfg.BrotliEnabled = true }
		if fileCfg.ETagEnabled   { cfg.ETagEnabled = true }
		if fileCfg.CacheModeEnabled { cfg.CacheModeEnabled = true }
		if fileCfg.MetricsEnabled   { cfg.MetricsEnabled = true }
	}

	currentCfgMu.Lock()
	currentCfg = cfg
	currentCfgMu.Unlock()

	// Log
	if cfg.LogFilePath != "" {
		f, err := os.OpenFile(cfg.LogFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil { log.Fatalf("Log: %v", err) }
		log.SetOutput(f)
	}

	// Cria dir se não existir
	if _, err := os.Stat(cfg.ServeDir); os.IsNotExist(err) {
		os.MkdirAll(cfg.ServeDir, 0755)
		logLine("Diretório '" + cfg.ServeDir + "' criado automaticamente.")
	}

	injJS  := readFileContent(cfg.InjectJSPath)
	injCSS := readFileContent(cfg.InjectCSSPath)

	// ── Process Supervisor ───────────────────────────────────────────────────────
	supervisor = NewProcessSupervisor()

	// PHP auto-start
	if *phpAutoFlag || cfg.FastCGI.AutoStart {
		if bin, args, ok := detectRuntime("php"); ok {
			cfg.FastCGI.Enabled = true
			if cfg.FastCGI.Address == "" {
				cfg.FastCGI.Address = "127.0.0.1:9000"
			}
			supervisor.Register(ProcessConfig{
				Name: "php-fpm", Command: bin, Args: args,
				AutoStart: true, AutoRestart: true, Port: 9000,
			})
			supervisor.Start("php-fpm")
		} else {
			logLine("PHP não encontrado no sistema.")
		}
	}

	// Python auto-start
	if *pythonFlag != "" {
		if bin, args, ok := detectRuntime("python"); ok {
			supervisor.Register(ProcessConfig{
				Name: "python", Command: bin, Args: args,
				AutoStart: true, AutoRestart: true, Port: 8000,
			})
			supervisor.Start("python")
		}
	}

	// Node auto-start
	if *nodeFlag != "" {
		if bin, _, ok := detectRuntime("node"); ok {
			supervisor.Register(ProcessConfig{
				Name: "node", Command: bin, Args: []string{"server.js"},
				Dir: *nodeFlag, AutoStart: true, AutoRestart: true, Port: 3000,
			})
			supervisor.Start("node")
		}
	}

	// Processos do config.json
	for _, proc := range cfg.Processes {
		supervisor.Register(proc)
		if proc.AutoStart {
			supervisor.Start(proc.Name)
		}
	}

	// Entradas /etc/hosts do config
	for _, entry := range cfg.HostsEntries {
		if err := addHostsEntry(entry); err != nil {
			logLine("Hosts: " + err.Error())
		}
	}

	// DNS local
	if cfg.DNS.Enabled {
		go startDNSServer(cfg.DNS)
	}

	go handleMessages()
	go watchFiles(cfg)

	// ── Roteador ─────────────────────────────────────────────────────────────────
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleConnections)

	if cfg.DashboardEnabled {
		mux.HandleFunc("/___brhttp", dashboardHandler)
		mux.HandleFunc("/___brhttp/", dashboardHandler)
		mux.HandleFunc("/___brhttp/metrics-json", metricsJSONHandler)
	}
	if cfg.MetricsEnabled {
		mux.HandleFunc("/metrics", metricsHandler)
	}

	// ── API ──────────────────────────────────────────────────────────────────────
	apiMux := http.NewServeMux()

	apiMux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" { http.Error(w, "405", 405); return }
		currentCfgMu.RLock()
		liveCfg := currentCfg
		currentCfgMu.RUnlock()
		clientsMu.Lock(); cc := len(clients); clientsMu.Unlock()
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
		if r.Method != "POST" { http.Error(w, "405", 405); return }
		msg, _ := json.Marshal(map[string]string{"type": "reload"})
		broadcast <- msg
		logLine("Live reload disparado via API")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	})

	apiMux.HandleFunc("/api/reload-config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" { http.Error(w, "405", 405); return }
		currentCfgMu.RLock()
		cfgPath := currentCfg.ConfigFilePath
		currentCfgMu.RUnlock()
		if cfgPath == "" { http.Error(w, `{"error":"no config file"}`, 400); return }
		newCfg := Config{ConfigFilePath: cfgPath}
		if err := loadConfig(cfgPath, &newCfg); err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, 500)
			return
		}
		currentCfgMu.Lock()
		currentCfg = newCfg
		currentCfgMu.Unlock()
		logLine("Config recarregada via API")
		w.Write([]byte(`{"ok":true}`))
	})

	// NOVO: toggle módulos em tempo real sem restart
	apiMux.HandleFunc("/api/set", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" { http.Error(w, "405", 405); return }
		var req struct {
			Module string `json:"module"`
			Value  bool   `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"bad request"}`, 400)
			return
		}
		currentCfgMu.Lock()
		switch req.Module {
		case "gzip":            currentCfg.GzipEnabled = req.Value
		case "brotli":          currentCfg.BrotliEnabled = req.Value
		case "spa":             currentCfg.SPAFallbackEnabled = req.Value
		case "fastcgi/php":     currentCfg.FastCGI.Enabled = req.Value
		case "rate_limit":      currentCfg.RateLimit.Enabled = req.Value
		case "etag":            currentCfg.ETagEnabled = req.Value
		case "cache":           currentCfg.CacheModeEnabled = req.Value
		case "métricas":        currentCfg.MetricsEnabled = req.Value
		case "https":           currentCfg.HTTPSEnabled = req.Value
		case "dns_local":       currentCfg.DNS.Enabled = req.Value
		}
		currentCfgMu.Unlock()
		logLine(fmt.Sprintf("Módulo '%s' → %v", req.Module, req.Value))
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	})

	// NOVO: gerenciamento de processos via API
	apiMux.HandleFunc("/api/process/start", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Name string `json:"name"` }
		json.NewDecoder(r.Body).Decode(&req)
		if err := supervisor.Start(req.Name); err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, 500)
			return
		}
		w.Write([]byte(`{"ok":true}`))
	})

	apiMux.HandleFunc("/api/process/stop", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Name string `json:"name"` }
		json.NewDecoder(r.Body).Decode(&req)
		supervisor.Stop(req.Name)
		w.Write([]byte(`{"ok":true}`))
	})

	apiMux.HandleFunc("/api/process/restart", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Name string `json:"name"` }
		json.NewDecoder(r.Body).Decode(&req)
		supervisor.Restart(req.Name)
		w.Write([]byte(`{"ok":true}`))
	})

	apiMux.HandleFunc("/api/process/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(supervisor.Status())
	})

	// NOVO: auto-detectar runtimes
	apiMux.HandleFunc("/api/process/autodetect", func(w http.ResponseWriter, r *http.Request) {
		detected := []string{}
		for _, rt := range []string{"php", "python", "node", "ruby"} {
			if bin, args, ok := detectRuntime(rt); ok {
				name := rt
				if _, exists := supervisor.processes[name]; !exists {
					port := map[string]int{"php": 9000, "python": 8000, "node": 3000, "ruby": 3000}[rt]
					supervisor.Register(ProcessConfig{
						Name: name, Command: bin, Args: args,
						AutoStart: false, AutoRestart: false, Port: port,
					})
					detected = append(detected, name+" ("+bin+")")
					logLine("Auto-detectado: " + name + " → " + bin)
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "detected": detected})
	})

	// NOVO: adicionar proxy rule dinamicamente
	apiMux.HandleFunc("/api/config/proxy/add", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Path   string `json:"path"`
			Target string `json:"target"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		currentCfgMu.Lock()
		currentCfg.ProxyRules = append(currentCfg.ProxyRules, ProxyRule{Path: req.Path, Target: req.Target})
		currentCfgMu.Unlock()
		logLine(fmt.Sprintf("Proxy adicionado: %s → %s", req.Path, req.Target))
		w.Write([]byte(`{"ok":true}`))
	})

	apiMux.HandleFunc("/api/config/proxy/delete", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Path string `json:"path"` }
		json.NewDecoder(r.Body).Decode(&req)
		currentCfgMu.Lock()
		var filtered []ProxyRule
		for _, p := range currentCfg.ProxyRules {
			if p.Path != req.Path { filtered = append(filtered, p) }
		}
		currentCfg.ProxyRules = filtered
		currentCfgMu.Unlock()
		logLine("Proxy removido: " + req.Path)
		w.Write([]byte(`{"ok":true}`))
	})

	// NOVO: gerenciar /etc/hosts
	apiMux.HandleFunc("/api/hosts/add", func(w http.ResponseWriter, r *http.Request) {
		var req HostsEntry
		json.NewDecoder(r.Body).Decode(&req)
		if req.IP == "" { req.IP = "127.0.0.1" }
		if err := addHostsEntry(req); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Write([]byte(`{"ok":true}`))
	})

	apiMux.HandleFunc("/api/hosts/delete", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Domain string `json:"domain"` }
		json.NewDecoder(r.Body).Decode(&req)
		if err := removeHostsEntry(req.Domain); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Write([]byte(`{"ok":true}`))
	})

	apiMux.HandleFunc("/api/hosts/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(listHostsEntries())
	})

	// API command
	apiMux.HandleFunc("/api/command", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" { http.Error(w, "405", 405); return }
		currentCfgMu.RLock()
		liveCfg := currentCfg
		currentCfgMu.RUnlock()
		if !liveCfg.APICommandEnabled { http.Error(w, `{"error":"disabled"}`, 403); return }
		var req struct { Command string `json:"command"`; Args []string `json:"args"` }
		json.NewDecoder(r.Body).Decode(&req)
		if len(liveCfg.APICommandAllowList) > 0 {
			allowed := false
			for _, c := range liveCfg.APICommandAllowList { if c == req.Command { allowed = true; break } }
			if !allowed { http.Error(w, `{"error":"not in allow list"}`, 403); return }
		}
		go func() {
			cmd := exec.Command(req.Command, req.Args...)
			cmd.Stdout = os.Stdout; cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil { logLine("Cmd erro: " + err.Error()) }
		}()
		w.Write([]byte(`{"ok":true}`))
	})

	mux.Handle("/api/", apiAuthMiddleware(cfg.APIToken, apiMux))

	// File server com config dinâmica
	buildHandler := func() http.Handler {
		currentCfgMu.RLock()
		liveCfg := currentCfg
		currentCfgMu.RUnlock()

		var fs http.Handler
		if liveCfg.DirListingEnabled {
			fs = http.FileServer(http.Dir(liveCfg.ServeDir))
		} else {
			fs = http.FileServer(noDirListingFS{http.Dir(liveCfg.ServeDir)})
		}

		h := fs
		h = customErrorPageMiddleware(liveCfg.Custom404PagePath, liveCfg.ServeDir, h)
		h = spaFallbackMiddleware(liveCfg.ServeDir, liveCfg.SPAFallbackEnabled, h)
		h = liveReloadInjector(injJS, injCSS, h)
		h = fastCGIMiddleware(liveCfg.FastCGI, liveCfg.ServeDir, h)
		h = mockRoutesMiddleware(liveCfg.MockRoutes, h)
		h = virtualHostMiddleware(liveCfg.VirtualHosts, h)
		h = reverseProxyMiddleware(liveCfg.ProxyRules, h)
		h = rewriteRedirectMiddleware(liveCfg.Rewrites, liveCfg.Redirects, h)
		h = customHeadersMiddleware(liveCfg.CustomHeaders, h)
		h = corsMiddleware(h)
		if !liveCfg.CacheModeEnabled { h = noCacheMiddleware(h) }
		h = gzipMiddleware(liveCfg.GzipEnabled, h)
		h = rateLimitMiddleware(liveCfg.RateLimit, h)
		h = loggingMiddleware(h)
		return h
	}

	// Handler dinâmico — relê config a cada request para refletir toggles em tempo real
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buildHandler().ServeHTTP(w, r)
	}))

	// ── Extra ports ──────────────────────────────────────────────────────────────
	for _, ep := range cfg.ExtraPorts {
		go func(ep ExtraPort) {
			epMux := http.NewServeMux()
			epMux.Handle("/", http.FileServer(http.Dir(ep.Dir)))
			addr := fmt.Sprintf(":%d", ep.Port)
			logLine(fmt.Sprintf("Porta extra HTTP em %s → %s", addr, ep.Dir))
			if ep.HTTPS {
				cert, _ := generateSelfSignedCert()
				srv := &http.Server{Addr: addr, Handler: epMux, TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}}}
				srv.ListenAndServeTLS("", "")
			} else {
				http.ListenAndServe(addr, epMux)
			}
		}(ep)
	}

	// ── SIGHUP hot reload ────────────────────────────────────────────────────────
	if cfg.ConfigFilePath != "" {
		sigHUP := make(chan os.Signal, 1)
		signal.Notify(sigHUP, syscall.SIGHUP)
		go func() {
			for range sigHUP {
				newCfg := Config{ConfigFilePath: cfg.ConfigFilePath}
				if err := loadConfig(cfg.ConfigFilePath, &newCfg); err != nil {
					logLine("SIGHUP config erro: " + err.Error())
					continue
				}
				currentCfgMu.Lock()
				currentCfg = newCfg
				currentCfgMu.Unlock()
				logLine("Config recarregada via SIGHUP")
			}
		}()
	}

	// ── Graceful shutdown ────────────────────────────────────────────────────────
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopCh
		logLine("Encerrando...")
		supervisor.StopAll()
		fireWebhooks("server_stop", map[string]string{
			"timestamp": time.Now().Format(time.RFC3339),
			"port":      strconv.Itoa(cfg.Port),
		}, cfg)
		os.Exit(0)
	}()

	// ── server_start webhooks ────────────────────────────────────────────────────
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
	if cfg.HTTPSEnabled { fmt.Printf("  🔒  HTTPS     → https://localhost:%d\n", cfg.HTTPSPort) }
	if cfg.DashboardEnabled { fmt.Printf("  📊  Dashboard → http://localhost:%d/___brhttp\n", cfg.Port) }
	if cfg.MetricsEnabled { fmt.Printf("  📈  Métricas  → http://localhost:%d/metrics\n", cfg.Port) }
	if cfg.FastCGI.Enabled { fmt.Printf("  🐘  FastCGI   → %s\n", cfg.FastCGI.Address) }
	if cfg.DNS.Enabled { fmt.Printf("  🌐  DNS local → UDP:%d\n", cfg.DNS.Port) }
	if len(cfg.ExtraPorts) > 0 {
		for _, ep := range cfg.ExtraPorts {
			fmt.Printf("  🔌  Porta extra → :%d → %s\n", ep.Port, ep.Dir)
		}
	}
	fmt.Printf("  📁  Servindo  → %s\n", cfg.ServeDir)
	fmt.Printf("  ⚡  Live Reload, Supervisor de processos: ativos\n")
	if cfg.APIToken == "" { fmt.Printf("  ⚠️   Defina --api-token para proteger a API\n") }
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
			if err != nil { log.Fatalf("TLS: %v", err) }
			tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}}
			logLine("Certificado TLS auto-assinado gerado (localhost)")
		}
		go func() {
			srv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.HTTPSPort), Handler: mux, TLSConfig: tlsCfg}
			logLine(fmt.Sprintf("HTTPS em https://localhost:%d", cfg.HTTPSPort))
			srv.ListenAndServeTLS("", "")
		}()
	}

	// ── HTTP ─────────────────────────────────────────────────────────────────────
	addr := fmt.Sprintf(":%d", cfg.Port)
	logLine(fmt.Sprintf("HTTP em http://localhost%s", addr))
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("HTTP erro: %v", err)
	}
}