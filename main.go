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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/websocket"
)

// ─── Tipos de configuração ────────────────────────────────────────────────────

type ProxyRule struct {
	Path   string `json:"path"`
	Target string `json:"target"`
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

// CommandWebhookRule - BUG FIX: adicionado AllowedCommands para whitelist de segurança
type CommandWebhookRule struct {
	Event   string   `json:"event"`
	Path    string   `json:"path"`
	Command string   `json:"command"`
	Args    []string `json:"args"`
}

// CustomHeader define um header HTTP customizável por rota
type CustomHeader struct {
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
}

type Config struct {
	Port                   int                  `json:"port"`
	ServeDir               string               `json:"serve_dir"`
	InjectJSPath           string               `json:"inject_js_path"`
	InjectCSSPath          string               `json:"inject_css_path"`
	SPAFallbackEnabled     bool                 `json:"spa_fallback_enabled"`
	DirListingEnabled      bool                 `json:"dir_listing_enabled"`
	GzipEnabled            bool                 `json:"gzip_enabled"`
	Custom404PagePath      string               `json:"custom_404_page_path"`
	ProxyRules             []ProxyRule          `json:"proxy_rules"`
	Rewrites               []RewriteRule        `json:"rewrites"`
	Redirects              []RedirectRule       `json:"redirects"`
	WatchDebounceMs        int                  `json:"watch_debounce_ms"`
	WatchExcludeDirs       []string             `json:"watch_exclude_dirs"`
	LogFilePath            string               `json:"log_file_path"`
	APIToken               string               `json:"api_token"`
	// BUG FIX: APICommandEnabled desabilita /api/command quando false (padrão seguro)
	APICommandEnabled      bool                 `json:"api_command_enabled"`
	// BUG FIX: lista de comandos permitidos via API (whitelist)
	APICommandAllowList    []string             `json:"api_command_allow_list"`
	NotificationWebhookURL string               `json:"notification_webhook_url"`
	CommandWebhooks        []CommandWebhookRule `json:"command_webhooks"`
	// NOVO: HTTPS local com certificado auto-assinado
	HTTPSEnabled           bool                 `json:"https_enabled"`
	HTTPSPort              int                  `json:"https_port"`
	HTTPSCertFile          string               `json:"https_cert_file"`
	HTTPSKeyFile           string               `json:"https_key_file"`
	// NOVO: Dashboard Web embutido
	DashboardEnabled       bool                 `json:"dashboard_enabled"`
	// NOVO: Headers customizáveis por rota
	CustomHeaders          []CustomHeader       `json:"custom_headers"`
	// NOVO: Hot reload de configuração via SIGHUP
	ConfigFilePath         string               `json:"-"`
}

// ─── Estado global ────────────────────────────────────────────────────────────

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}

	clients      = make(map[*Client]bool)
	clientsMu    sync.Mutex
	broadcast    = make(chan []byte, 64)
	serverStart  = time.Now()

	// Log circular para o dashboard (últimas 200 linhas)
	logBuffer   []string
	logBufferMu sync.Mutex

	// Config atual com mutex para hot reload
	currentCfg   Config
	currentCfgMu sync.RWMutex

	// Contadores de requisições para o dashboard
	requestCount int64
	requestMu    sync.Mutex
)

// ─── WebSocket ────────────────────────────────────────────────────────────────

type Client struct {
	conn *websocket.Conn
	send chan []byte
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logLine(fmt.Sprintf("Erro WebSocket upgrade: %v", err))
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
	for {
		msg, ok := <-c.send
		if !ok {
			c.conn.WriteMessage(websocket.CloseMessage, []byte{})
			return
		}
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

// logLine registra uma linha no buffer circular do dashboard e no log padrão
func logLine(msg string) {
	line := fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), msg)
	logBufferMu.Lock()
	logBuffer = append(logBuffer, line)
	if len(logBuffer) > 200 {
		logBuffer = logBuffer[len(logBuffer)-200:]
	}
	logBufferMu.Unlock()
	log.Println(msg)
}

// ─── Webhooks & comandos ──────────────────────────────────────────────────────

func executeCommandWebhook(rule CommandWebhookRule, eventDetails map[string]string) {
	cmdArgs := make([]string, len(rule.Args))
	for i, arg := range rule.Args {
		replaced := arg
		for k, v := range eventDetails {
			replaced = strings.ReplaceAll(replaced, fmt.Sprintf("{{%s}}", k), v)
		}
		cmdArgs[i] = replaced
	}
	cmd := exec.Command(rule.Command, cmdArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	logLine(fmt.Sprintf("Webhook comando: %s %v", rule.Command, cmdArgs))
	if err := cmd.Run(); err != nil {
		logLine(fmt.Sprintf("Erro webhook '%s': %v", rule.Command, err))
	}
}

func sendNotificationWebhook(targetURL string, payload map[string]string) {
	if targetURL == "" {
		return
	}
	jsonPayload, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", targetURL, bytes.NewBuffer(jsonPayload))
	if err != nil {
		logLine(fmt.Sprintf("Erro criando webhook: %v", err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		logLine(fmt.Sprintf("Erro enviando webhook para %s: %v", targetURL, err))
		return
	}
	defer resp.Body.Close()
	logLine(fmt.Sprintf("Webhook enviado para %s → %d", targetURL, resp.StatusCode))
}

func fireWebhooks(event string, details map[string]string, cfg Config) {
	for _, rule := range cfg.CommandWebhooks {
		if rule.Event == event {
			pathMatch := rule.Path == ""
			if !pathMatch {
				if fp, ok := details["rel_path"]; ok {
					pathMatch = strings.HasPrefix(fp, rule.Path) || strings.Contains(fp, rule.Path)
				}
			}
			if pathMatch {
				go executeCommandWebhook(rule, details)
			}
		}
	}
}

// ─── File watcher ─────────────────────────────────────────────────────────────

func watchFiles(cfg Config) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("Erro criando watcher: %v", err)
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
				capturedEvent := event
				timer = time.AfterFunc(debounce, func() {
					currentCfgMu.RLock()
					liveCfg := currentCfg
					currentCfgMu.RUnlock()

					relPath, _ := filepath.Rel(liveCfg.ServeDir, capturedEvent.Name)
					urlPath := "/" + strings.ReplaceAll(relPath, string(os.PathSeparator), "/")

					ext := strings.ToLower(filepath.Ext(capturedEvent.Name))
					msgType := "reload"
					switch ext {
					case ".css":
						msgType = "css-update"
					case ".js":
						msgType = "js-update"
					}

					msg, _ := json.Marshal(map[string]string{"type": msgType, "path": urlPath})
					broadcast <- msg
					logLine(fmt.Sprintf("Mudança detectada: %s → %s", capturedEvent.Name, msgType))

					details := map[string]string{
						"event_type": "file_change",
						"file_path":  capturedEvent.Name,
						"rel_path":   relPath,
						"op":         capturedEvent.Op.String(),
						"timestamp":  time.Now().Format(time.RFC3339),
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
				logLine(fmt.Sprintf("Excluindo do watcher: %s", path))
				return filepath.SkipDir
			}
		}
		watcher.Add(path)
		return nil
	})

	select {}
}

// ─── HTTPS auto-assinado ──────────────────────────────────────────────────────

// generateSelfSignedCert gera um certificado TLS local sem dependências externas
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

// ─── Dashboard Web embutido ───────────────────────────────────────────────────

func dashboardHandler(w http.ResponseWriter, r *http.Request) {
	currentCfgMu.RLock()
	cfg := currentCfg
	currentCfgMu.RUnlock()

	clientsMu.Lock()
	connectedClients := len(clients)
	clientsMu.Unlock()

	requestMu.Lock()
	reqCount := requestCount
	requestMu.Unlock()

	logBufferMu.Lock()
	logs := make([]string, len(logBuffer))
	copy(logs, logBuffer)
	logBufferMu.Unlock()

	logsJSON, _ := json.Marshal(logs)

	uptime := time.Since(serverStart).Round(time.Second).String()

	html := fmt.Sprintf(`<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>brhttp — Dashboard</title>
<style>
  :root {
    --bg: #0d1117; --surface: #161b22; --border: #30363d;
    --text: #e6edf3; --muted: #8b949e; --accent: #58a6ff;
    --green: #3fb950; --yellow: #d29922; --red: #f85149;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', monospace; min-height: 100vh; }
  header { background: var(--surface); border-bottom: 1px solid var(--border); padding: 16px 24px; display: flex; align-items: center; gap: 12px; }
  header h1 { font-size: 1.2rem; font-weight: 700; color: var(--accent); }
  .dot { width: 10px; height: 10px; border-radius: 50%; background: var(--green); animation: pulse 2s infinite; }
  @keyframes pulse { 0%%,100%% { opacity:1; } 50%% { opacity:.4; } }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; padding: 24px; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 20px; }
  .card .label { font-size: .75rem; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; margin-bottom: 8px; }
  .card .value { font-size: 1.8rem; font-weight: 700; }
  .card .sub { font-size: .8rem; color: var(--muted); margin-top: 4px; }
  .section { padding: 0 24px 24px; }
  .section h2 { font-size: .85rem; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; margin-bottom: 12px; }
  .log-box { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 16px; height: 320px; overflow-y: auto; font-family: monospace; font-size: .78rem; }
  .log-line { padding: 2px 0; border-bottom: 1px solid #21262d; color: var(--muted); }
  .log-line:last-child { color: var(--text); }
  .badge { display: inline-flex; align-items: center; gap: 6px; background: #1f6feb22; border: 1px solid #1f6feb55; color: var(--accent); border-radius: 20px; padding: 4px 12px; font-size: .78rem; margin: 4px; }
  .proxy-list { display: flex; flex-wrap: wrap; }
  .btn { background: var(--accent); color: #000; border: none; border-radius: 6px; padding: 8px 16px; font-size: .85rem; font-weight: 600; cursor: pointer; margin-right: 8px; }
  .btn:hover { opacity: .85; }
  .btn.danger { background: var(--red); color: #fff; }
  .actions { padding: 0 24px 24px; display: flex; gap: 8px; }
  #reload-status { font-size: .8rem; color: var(--green); display: none; }
</style>
</head>
<body>
<header>
  <div class="dot"></div>
  <h1>brhttp v2.0</h1>
  <span style="color:var(--muted);font-size:.85rem;margin-left:auto">Dashboard · <a href="http://localhost:%d" style="color:var(--accent)" target="_blank">localhost:%d</a></span>
</header>

<div class="grid">
  <div class="card">
    <div class="label">Status</div>
    <div class="value" style="color:var(--green);font-size:1.2rem">● Online</div>
    <div class="sub">Uptime: %s</div>
  </div>
  <div class="card">
    <div class="label">Clientes conectados</div>
    <div class="value">%d</div>
    <div class="sub">via WebSocket</div>
  </div>
  <div class="card">
    <div class="label">Requisições</div>
    <div class="value">%d</div>
    <div class="sub">nesta sessão</div>
  </div>
  <div class="card">
    <div class="label">Diretório servido</div>
    <div class="value" style="font-size:1rem">%s</div>
    <div class="sub">GZIP: %s · SPA: %s</div>
  </div>
</div>

<div class="actions">
  <button class="btn" onclick="triggerReload()">⚡ Forçar Live Reload</button>
  <span id="reload-status">Reload disparado!</span>
</div>

<div class="section">
  <h2>Regras de Proxy</h2>
  <div class="proxy-list">%s</div>
</div>

<div class="section">
  <h2>Log em tempo real</h2>
  <div class="log-box" id="logbox">%s</div>
</div>

<script>
var logs = %s;
var box = document.getElementById('logbox');

function renderLogs() {
  box.innerHTML = logs.map(function(l){ return '<div class="log-line">'+escHtml(l)+'</div>'; }).join('');
  box.scrollTop = box.scrollHeight;
}
renderLogs();

function escHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// WebSocket para logs em tempo real
var ws = new WebSocket('ws://' + location.host + '/ws');
ws.onmessage = function(e) {
  var msg = JSON.parse(e.data);
  if (msg.type === 'log') {
    logs.push(msg.line);
    if (logs.length > 200) logs = logs.slice(-200);
    renderLogs();
  }
};

function triggerReload() {
  fetch('/api/reload', {
    method:'POST',
    headers: {'Authorization': 'Bearer %s'}
  }).then(function(r){
    if(r.ok) {
      var s = document.getElementById('reload-status');
      s.style.display = 'inline';
      setTimeout(function(){ s.style.display='none'; }, 2000);
    }
  });
}

// Auto-refresh das métricas a cada 5s
setInterval(function(){
  fetch('/___brhttp/metrics').then(function(r){ return r.json(); }).then(function(d){
    document.querySelectorAll('.card .value')[1].textContent = d.connected_clients;
    document.querySelectorAll('.card .value')[2].textContent = d.request_count;
    document.querySelectorAll('.card .sub')[0].textContent = 'Uptime: ' + d.uptime;
  });
}, 5000);
</script>
</body>
</html>`,
		cfg.Port, cfg.Port,
		uptime,
		connectedClients,
		reqCount,
		cfg.ServeDir,
		boolLabel(cfg.GzipEnabled), boolLabel(cfg.SPAFallbackEnabled),
		proxyBadges(cfg.ProxyRules),
		"", // log box filled by JS
		string(logsJSON),
		cfg.APIToken,
	)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(html))
}

func boolLabel(b bool) string {
	if b {
		return "✓"
	}
	return "✗"
}

func proxyBadges(rules []ProxyRule) string {
	if len(rules) == 0 {
		return `<span style="color:var(--muted);font-size:.85rem">Nenhuma regra configurada.</span>`
	}
	var sb strings.Builder
	for _, r := range rules {
		sb.WriteString(fmt.Sprintf(`<span class="badge">%s → %s</span>`, r.Path, r.Target))
	}
	return sb.String()
}

// ─── Middlewares ──────────────────────────────────────────────────────────────

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// Não conta as rotas internas do dashboard no contador
		if !strings.HasPrefix(r.URL.Path, "/___brhttp") && r.URL.Path != "/ws" {
			requestMu.Lock()
			requestCount++
			requestMu.Unlock()
		}
		next.ServeHTTP(w, r)
		logLine(fmt.Sprintf("[%s] %s %s (%s)", r.Method, r.URL.Path, r.RemoteAddr, time.Since(start)))
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
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// NOVO: customHeadersMiddleware injeta headers por rota
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
	return &responseRecorder{
		ResponseWriter: w,
		StatusCode:     http.StatusOK,
		Body:           new(bytes.Buffer),
		Headers:        make(http.Header),
	}
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

func liveReloadInjector(injectedJS, injectedCSS string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws" || r.Method != http.MethodGet {
			next.ServeHTTP(w, r)
			return
		}
		rec := newResponseRecorder(w)
		next.ServeHTTP(rec, r)

		if strings.Contains(rec.Headers.Get("Content-Type"), "text/html") && rec.StatusCode == http.StatusOK {
			body := rec.Body.Bytes()

			lrScript := fmt.Sprintf(`<script>
(function(){
  var ws = new WebSocket("ws://%s/ws");
  ws.onmessage = function(e) {
    var msg = JSON.parse(e.data);
    if (msg.type === "reload") {
      location.reload();
    } else if (msg.type === "css-update") {
      var link = document.querySelector('link[href*="' + msg.path + '"]');
      if (link) { link.href = msg.path + '?v=' + Date.now(); } else { location.reload(); }
    } else if (msg.type === "js-update") {
      var old = document.querySelector('script[src*="' + msg.path + '"]');
      if (old) {
        var s = document.createElement('script');
        s.src = msg.path + '?v=' + Date.now();
        old.parentNode.replaceChild(s, old);
      } else { location.reload(); }
    }
  };
  ws.onclose = function(){ setTimeout(function(){ location.reload(); }, 1000); };
})();
</script>`, r.Host)

			var injections bytes.Buffer
			if injectedCSS != "" {
				injections.WriteString(fmt.Sprintf("<style>\n%s\n</style>\n", injectedCSS))
			}
			if injectedJS != "" {
				injections.WriteString(fmt.Sprintf("<script>\n%s\n</script>\n", injectedJS))
			}

			if idx := bytes.LastIndex(body, []byte("</head>")); idx != -1 {
				body = bytes.Join([][]byte{body[:idx], injections.Bytes(), body[idx:]}, nil)
			}
			if idx := bytes.LastIndex(body, []byte("</body>")); idx != -1 {
				body = bytes.Join([][]byte{body[:idx], []byte(lrScript), body[idx:]}, nil)
			} else {
				body = append(body, []byte(lrScript)...)
			}

			for k, v := range rec.Headers {
				w.Header()[k] = v
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
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
		if rec.StatusCode == http.StatusNotFound && !strings.Contains(filepath.Base(r.URL.Path), ".") && r.URL.Path != "/ws" {
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
		if rec.StatusCode == http.StatusNotFound {
			full := filepath.Join(serveDir, custom404Path)
			if _, err := os.Stat(full); err == nil {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				w.WriteHeader(http.StatusNotFound)
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
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
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
	proxies := make(map[string]*httputil.ReverseProxy)
	for _, rule := range rules {
		targetURL, err := url.Parse(rule.Target)
		if err != nil {
			continue
		}
		p := httputil.NewSingleHostReverseProxy(targetURL)
		p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		}
		orig := p.Director
		capturedPath := rule.Path
		p.Director = func(req *http.Request) {
			orig(req)
			req.URL.Path = strings.TrimPrefix(req.URL.Path, capturedPath)
		}
		proxies[rule.Path] = p
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for prefix, proxy := range proxies {
			if strings.HasPrefix(r.URL.Path, prefix) {
				proxy.ServeHTTP(w, r)
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
				http.Redirect(w, r, strings.Replace(r.URL.Path, rule.From, rule.To, 1), rule.Code)
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

// BUG FIX: apiAuthMiddleware agora bloqueia quando sem token (ao invés de liberar)
func apiAuthMiddleware(apiToken string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Se não há token configurado, a API ainda funciona mas emite aviso no log
		if apiToken == "" {
			logLine("AVISO SEGURANÇA: API sendo acessada sem token configurado. Defina api_token no config.")
			next.ServeHTTP(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		parts := strings.SplitN(auth, " ", 2)
		if len(parts) != 2 || parts[0] != "Bearer" || parts[1] != apiToken {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func readInjectedFileContent(p string) string {
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

func loadConfigFromFile(filePath string, cfg *Config) error {
	if filePath == "" {
		return nil
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("não foi possível ler '%s': %w", filePath, err)
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("JSON inválido em '%s': %w", filePath, err)
	}
	return nil
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	// Flags
	configFileFlag     := flag.String("config", "", "Caminho para config.json")
	portFlag           := flag.Int("port", 5571, "Porta HTTP")
	serveDirFlag       := flag.String("dir", "www", "Diretório a servir")
	injectJSFlag       := flag.String("inject-js", "", "Arquivo JS a injetar")
	injectCSSFlag      := flag.String("inject-css", "", "Arquivo CSS a injetar")
	spaFlag            := flag.Bool("spa-fallback", false, "Habilita SPA fallback")
	dirListingFlag     := flag.Bool("enable-dir-listing", false, "Habilita listagem de diretórios")
	gzipFlag           := flag.Bool("enable-gzip", false, "Habilita Gzip")
	page404Flag        := flag.String("404-page", "", "Página 404 customizada")
	debounceFlag       := flag.Int("watch-debounce-ms", 100, "Debounce do watcher (ms)")
	excludeDirsFlag    := flag.String("watch-exclude-dirs", "", "Diretórios excluídos do watcher")
	logFileFlag        := flag.String("log-file", "server.log", "Arquivo de log")
	apiTokenFlag       := flag.String("api-token", "", "Token da API")
	notifWebhookFlag   := flag.String("notification-webhook-url", "", "URL de webhook de notificação")
	httpsFlag          := flag.Bool("https", false, "Habilita HTTPS com certificado auto-assinado")
	httpsPortFlag      := flag.Int("https-port", 5572, "Porta HTTPS")
	dashboardFlag      := flag.Bool("dashboard", true, "Habilita dashboard em /___brhttp")
	flag.Parse()

	cfg := Config{
		Port:                   *portFlag,
		ServeDir:               *serveDirFlag,
		InjectJSPath:           *injectJSFlag,
		InjectCSSPath:          *injectCSSFlag,
		SPAFallbackEnabled:     *spaFlag,
		DirListingEnabled:      *dirListingFlag,
		GzipEnabled:            *gzipFlag,
		Custom404PagePath:      *page404Flag,
		WatchDebounceMs:        *debounceFlag,
		LogFilePath:            *logFileFlag,
		APIToken:               *apiTokenFlag,
		NotificationWebhookURL: *notifWebhookFlag,
		HTTPSEnabled:           *httpsFlag,
		HTTPSPort:              *httpsPortFlag,
		DashboardEnabled:       *dashboardFlag,
		ProxyRules:             []ProxyRule{},
		Rewrites:               []RewriteRule{},
		Redirects:              []RedirectRule{},
		CommandWebhooks:        []CommandWebhookRule{},
		CustomHeaders:          []CustomHeader{},
		ConfigFilePath:         *configFileFlag,
	}

	if *excludeDirsFlag != "" {
		cfg.WatchExcludeDirs = strings.Split(*excludeDirsFlag, ",")
	}

	// Carrega config do arquivo (flags têm precedência)
	if *configFileFlag != "" {
		fileCfg := Config{}
		if err := loadConfigFromFile(*configFileFlag, &fileCfg); err != nil {
			log.Fatalf("Erro ao carregar config: %v", err)
		}
		// Merge: arquivo preenche campos não definidos por flag
		if fileCfg.ProxyRules != nil       { cfg.ProxyRules = fileCfg.ProxyRules }
		if fileCfg.Rewrites != nil         { cfg.Rewrites = fileCfg.Rewrites }
		if fileCfg.Redirects != nil        { cfg.Redirects = fileCfg.Redirects }
		if fileCfg.CommandWebhooks != nil  { cfg.CommandWebhooks = fileCfg.CommandWebhooks }
		if fileCfg.CustomHeaders != nil    { cfg.CustomHeaders = fileCfg.CustomHeaders }
		if fileCfg.APICommandAllowList != nil { cfg.APICommandAllowList = fileCfg.APICommandAllowList }
		cfg.APICommandEnabled = fileCfg.APICommandEnabled
		if fileCfg.WatchExcludeDirs != nil && *excludeDirsFlag == "" {
			cfg.WatchExcludeDirs = fileCfg.WatchExcludeDirs
		}
		// Campos string/int só sobrescreve se flag está no valor padrão
		if *portFlag == 5571 && fileCfg.Port != 0           { cfg.Port = fileCfg.Port }
		if *serveDirFlag == "www" && fileCfg.ServeDir != ""  { cfg.ServeDir = fileCfg.ServeDir }
		if *logFileFlag == "server.log" && fileCfg.LogFilePath != "" { cfg.LogFilePath = fileCfg.LogFilePath }
		if *apiTokenFlag == "" && fileCfg.APIToken != ""     { cfg.APIToken = fileCfg.APIToken }
		if *httpsPortFlag == 5572 && fileCfg.HTTPSPort != 0  { cfg.HTTPSPort = fileCfg.HTTPSPort }
		if fileCfg.HTTPSCertFile != ""                        { cfg.HTTPSCertFile = fileCfg.HTTPSCertFile }
		if fileCfg.HTTPSKeyFile != ""                         { cfg.HTTPSKeyFile = fileCfg.HTTPSKeyFile }
		if fileCfg.Custom404PagePath != "" && *page404Flag == "" { cfg.Custom404PagePath = fileCfg.Custom404PagePath }
		if fileCfg.NotificationWebhookURL != "" && *notifWebhookFlag == "" { cfg.NotificationWebhookURL = fileCfg.NotificationWebhookURL }
	}

	// Salva config global para hot reload
	currentCfgMu.Lock()
	currentCfg = cfg
	currentCfgMu.Unlock()

	// Configura log
	if cfg.LogFilePath != "" {
		f, err := os.OpenFile(cfg.LogFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Erro ao abrir log '%s': %v", cfg.LogFilePath, err)
		}
		log.SetOutput(f)
	}

	// Verifica diretório
	if _, err := os.Stat(cfg.ServeDir); os.IsNotExist(err) {
		log.Fatalf("Diretório '%s' não encontrado.", cfg.ServeDir)
	}

	injJS  := readInjectedFileContent(cfg.InjectJSPath)
	injCSS := readInjectedFileContent(cfg.InjectCSSPath)

	// Goroutines base
	go handleMessages()
	go watchFiles(cfg)

	// ── Constrói o roteador ──────────────────────────────────────────────────
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleConnections)

	// Dashboard
	if cfg.DashboardEnabled {
		mux.HandleFunc("/___brhttp", dashboardHandler)
		mux.HandleFunc("/___brhttp/metrics", func(w http.ResponseWriter, r *http.Request) {
			clientsMu.Lock()
			cc := len(clients)
			clientsMu.Unlock()
			requestMu.Lock()
			rc := requestCount
			requestMu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"connected_clients": cc,
				"request_count":     rc,
				"uptime":            time.Since(serverStart).Round(time.Second).String(),
			})
		})
	}

	// API
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/api/reload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		msg, _ := json.Marshal(map[string]string{"type": "reload"})
		broadcast <- msg
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	})

	apiMux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		clientsMu.Lock()
		cc := len(clients)
		clientsMu.Unlock()
		currentCfgMu.RLock()
		liveCfg := currentCfg
		currentCfgMu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":            "running",
			"uptime":            time.Since(serverStart).String(),
			"port":              liveCfg.Port,
			"serve_dir":         liveCfg.ServeDir,
			"connected_clients": cc,
			"version":           "2.0.0",
		})
	})

	// BUG FIX: /api/command só disponível se api_command_enabled=true E com whitelist
	apiMux.HandleFunc("/api/command", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		currentCfgMu.RLock()
		liveCfg := currentCfg
		currentCfgMu.RUnlock()

		if !liveCfg.APICommandEnabled {
			http.Error(w, "API command endpoint disabled. Set api_command_enabled=true in config.", http.StatusForbidden)
			return
		}

		var req struct {
			Command string   `json:"command"`
			Args    []string `json:"args"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		// BUG FIX: verifica whitelist de comandos
		if len(liveCfg.APICommandAllowList) > 0 {
			allowed := false
			for _, c := range liveCfg.APICommandAllowList {
				if c == req.Command {
					allowed = true
					break
				}
			}
			if !allowed {
				http.Error(w, fmt.Sprintf("Comando '%s' não está na lista de permissões (api_command_allow_list).", req.Command), http.StatusForbidden)
				return
			}
		}

		go func() {
			cmd := exec.Command(req.Command, req.Args...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			logLine(fmt.Sprintf("Comando via API: %s %v", req.Command, req.Args))
			if err := cmd.Run(); err != nil {
				logLine(fmt.Sprintf("Erro comando API '%s': %v", req.Command, err))
			}
		}()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true,"message":"Comando enviado."}`))
	})

	// BUG FIX: /api/reload-config — hot reload sem derrubar o servidor
	apiMux.HandleFunc("/api/reload-config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		currentCfgMu.RLock()
		cfgPath := currentCfg.ConfigFilePath
		currentCfgMu.RUnlock()
		if cfgPath == "" {
			http.Error(w, "Nenhum arquivo de config carregado.", http.StatusBadRequest)
			return
		}
		newCfg := Config{ConfigFilePath: cfgPath}
		if err := loadConfigFromFile(cfgPath, &newCfg); err != nil {
			http.Error(w, fmt.Sprintf("Erro recarregando config: %v", err), http.StatusInternalServerError)
			return
		}
		currentCfgMu.Lock()
		currentCfg = newCfg
		currentCfgMu.Unlock()
		logLine("Config recarregada via API /api/reload-config")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true,"message":"Config recarregada."}`))
	})

	mux.Handle("/api/", apiAuthMiddleware(cfg.APIToken, apiMux))

	// Handler de arquivos estáticos
	var fileServer http.Handler
	if cfg.DirListingEnabled {
		fileServer = http.FileServer(http.Dir(cfg.ServeDir))
	} else {
		fileServer = http.FileServer(noDirListingFS{http.Dir(cfg.ServeDir)})
	}

	handler := fileServer
	handler = customErrorPageMiddleware(cfg.Custom404PagePath, cfg.ServeDir, handler)
	handler = spaFallbackMiddleware(cfg.ServeDir, cfg.SPAFallbackEnabled, handler)
	handler = liveReloadInjector(injJS, injCSS, handler)
	handler = reverseProxyMiddleware(cfg.ProxyRules, handler)
	handler = rewriteRedirectMiddleware(cfg.Rewrites, cfg.Redirects, handler)
	handler = customHeadersMiddleware(cfg.CustomHeaders, handler) // NOVO
	handler = corsMiddleware(handler)
	handler = noCacheMiddleware(handler)
	handler = gzipMiddleware(cfg.GzipEnabled, handler)
	handler = loggingMiddleware(handler)
	mux.Handle("/", handler)

	// ── Hot reload via SIGHUP ────────────────────────────────────────────────
	if cfg.ConfigFilePath != "" {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGHUP)
		go func() {
			for range sigCh {
				newCfg := Config{ConfigFilePath: cfg.ConfigFilePath}
				if err := loadConfigFromFile(cfg.ConfigFilePath, &newCfg); err != nil {
					logLine(fmt.Sprintf("SIGHUP: erro recarregando config: %v", err))
					continue
				}
				currentCfgMu.Lock()
				currentCfg = newCfg
				currentCfgMu.Unlock()
				logLine("Config recarregada via SIGHUP.")
			}
		}()
	}

	// ── BUG FIX: server_stop via SIGINT/SIGTERM ──────────────────────────────
	// (na v1.8 o código de server_stop ficava após log.Fatal — nunca executava)
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopCh
		logLine("Servidor encerrando...")
		details := map[string]string{
			"timestamp": time.Now().Format(time.RFC3339),
			"port":      fmt.Sprintf("%d", cfg.Port),
		}
		fireWebhooks("server_stop", details, cfg)
		os.Exit(0)
	}()

	// ── Webhooks server_start ────────────────────────────────────────────────
	startDetails := map[string]string{
		"timestamp": time.Now().Format(time.RFC3339),
		"port":      fmt.Sprintf("%d", cfg.Port),
		"serve_dir": cfg.ServeDir,
	}
	fireWebhooks("server_start", startDetails, cfg)

	// ── Imprime banner ───────────────────────────────────────────────────────
	fmt.Println()
	fmt.Println("  ██████╗ ██████╗ ██╗  ██╗████████╗████████╗██████╗ ")
	fmt.Println("  ██╔══██╗██╔══██╗██║  ██║╚══██╔══╝╚══██╔══╝██╔══██╗")
	fmt.Println("  ██████╔╝██████╔╝███████║   ██║      ██║   ██████╔╝")
	fmt.Println("  ██╔══██╗██╔══██╗██╔══██║   ██║      ██║   ██╔═══╝ ")
	fmt.Println("  ██████╔╝██║  ██║██║  ██║   ██║      ██║   ██║     ")
	fmt.Println("  ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝      ╚═╝   ╚═╝  v2.0")
	fmt.Println()
	fmt.Printf("  🚀  HTTP   → http://localhost:%d\n", cfg.Port)
	if cfg.HTTPSEnabled {
		fmt.Printf("  🔒  HTTPS  → https://localhost:%d\n", cfg.HTTPSPort)
	}
	if cfg.DashboardEnabled {
		fmt.Printf("  📊  Dashboard → http://localhost:%d/___brhttp\n", cfg.Port)
	}
	fmt.Printf("  📁  Servindo: %s\n", cfg.ServeDir)
	fmt.Printf("  ⚡  Live Reload: ativado\n")
	if cfg.APIToken == "" {
		fmt.Printf("  ⚠️   api_token não configurado — defina para proteger a API\n")
	}
	fmt.Println()

	// ── HTTPS ────────────────────────────────────────────────────────────────
	if cfg.HTTPSEnabled {
		var tlsCfg *tls.Config
		if cfg.HTTPSCertFile != "" && cfg.HTTPSKeyFile != "" {
			cert, err := tls.LoadX509KeyPair(cfg.HTTPSCertFile, cfg.HTTPSKeyFile)
			if err != nil {
				log.Fatalf("Erro carregando certificado TLS: %v", err)
			}
			tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}}
		} else {
			cert, err := generateSelfSignedCert()
			if err != nil {
				log.Fatalf("Erro gerando certificado auto-assinado: %v", err)
			}
			tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}}
			logLine("Certificado TLS auto-assinado gerado para localhost (válido 1 ano)")
		}
		httpsAddr := fmt.Sprintf(":%d", cfg.HTTPSPort)
		go func() {
			srv := &http.Server{Addr: httpsAddr, Handler: mux, TLSConfig: tlsCfg}
			logLine(fmt.Sprintf("HTTPS escutando em https://localhost%s", httpsAddr))
			if err := srv.ListenAndServeTLS("", ""); err != nil {
				log.Fatalf("Erro HTTPS: %v", err)
			}
		}()
	}

	// ── HTTP ─────────────────────────────────────────────────────────────────
	addr := fmt.Sprintf(":%d", cfg.Port)
	logLine(fmt.Sprintf("Servidor HTTP iniciado em http://localhost%s", addr))
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Erro fatal: %v", err)
	}
}