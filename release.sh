#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
#  brhttp — build & release script
#  Uso: ./release.sh
#  Requer: go, git, gh (GitHub CLI)
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

VERSION="4.5.0"
TAG="v${VERSION}"
BINARY="brhttp"
REPO="henriquetourinho/brhttp"   # ← ajuste se necessário
RELEASE_DIR="./release"
NOTES_FILE="${RELEASE_DIR}/NOTES.md"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Pré-requisitos ────────────────────────────────────────────────────────────
command -v go  >/dev/null 2>&1 || error "go não encontrado. Instale em https://go.dev/dl/"
command -v git >/dev/null 2>&1 || error "git não encontrado."
command -v gh  >/dev/null 2>&1 || warn "gh (GitHub CLI) não encontrado — release GitHub será pulado."

info "brhttp release pipeline — ${TAG}"
echo

# ── Limpeza ───────────────────────────────────────────────────────────────────
rm -rf "${RELEASE_DIR}"
mkdir -p "${RELEASE_DIR}"

# ── go mod tidy ───────────────────────────────────────────────────────────────
info "Atualizando dependências..."
go mod tidy
ok "go mod tidy concluído"

# ── Build multiplataforma ─────────────────────────────────────────────────────
info "Iniciando builds cross-platform..."

declare -A TARGETS=(
  ["linux/amd64"]="${BINARY}-linux-amd64"
  ["linux/arm64"]="${BINARY}-linux-arm64"
  ["linux/arm"]="${BINARY}-linux-arm"
  ["darwin/amd64"]="${BINARY}-darwin-amd64"
  ["darwin/arm64"]="${BINARY}-darwin-arm64"
  ["windows/amd64"]="${BINARY}-windows-amd64.exe"
  ["windows/arm64"]="${BINARY}-windows-arm64.exe"
)

BUILT=()
for target in "${!TARGETS[@]}"; do
  OS="${target%/*}"
  ARCH="${target#*/}"
  OUT="${RELEASE_DIR}/${TARGETS[$target]}"
  echo -n "  Building ${OS}/${ARCH}... "
  if GOOS="${OS}" GOARCH="${ARCH}" CGO_ENABLED=0 \
     go build \
       -ldflags="-s -w -X main.Version=${VERSION}" \
       -trimpath \
       -o "${OUT}" \
       . 2>/dev/null; then
    SIZE=$(du -sh "${OUT}" | cut -f1)
    echo -e "${GREEN}✓${NC} ${OUT} (${SIZE})"
    BUILT+=("${OUT}")
  else
    echo -e "${RED}✗${NC} falhou"
  fi
done

ok "Builds concluídos: ${#BUILT[@]} binários"

# ── Checksums ─────────────────────────────────────────────────────────────────
info "Gerando checksums SHA256..."
pushd "${RELEASE_DIR}" > /dev/null
sha256sum ${BINARY}-* > SHA256SUMS.txt 2>/dev/null || shasum -a 256 ${BINARY}-* > SHA256SUMS.txt
ok "SHA256SUMS.txt gerado"
popd > /dev/null

# ── Release notes ─────────────────────────────────────────────────────────────
cat > "${NOTES_FILE}" << 'NOTES'
## brhttp v4.5.0

### 🆕 Novidades

#### 🕐 Scheduler / Cron Nativo
- Sintaxe cron padrão (`* * * * *`) sem dependência do cron do sistema
- Funciona em Linux, macOS e Windows
- Timeout por job configurável
- Notificação automática em caso de erro (`on_error: "notify"`)
- Histórico de execuções no dashboard
- API: `/api/scheduler/list`, `/add`, `/delete`, `/run`, `/history`
- Botão "▶ Rodar agora" no dashboard

#### 📨 Notificações via Telegram e Discord
- **Telegram**: suporte a múltiplos `chat_ids` (usuários e grupos)
- **Discord**: suporte a múltiplos webhooks
- Notificações automáticas em: server start/stop, erros de job, timeout
- Teste de envio via dashboard (`/___brhttp` → Notificações → Testar Envio)
- Configurável via `config.json` → `notifications.telegram` / `notifications.discord`

#### 📝 Log por Virtual Host
- Cada virtual host tem seu próprio arquivo de log
- Configure `vhost_log_dir` no `config.json`
- Ex: `"vhost_log_dir": "./logs/vhosts"` → gera `./logs/vhosts/meusite.local.log`

#### 🔑 Basic Auth Global
- Proteção por usuário/senha em toda a aplicação
- Configure via `config.json` → `basic_auth`
- Compatível com todos os browsers e ferramentas HTTP

#### 🔐 Let's Encrypt (ACME) — Base
- Estrutura e configuração prontas para ACME HTTP-01
- Para ativar completamente: `go get golang.org/x/crypto` e habilite `lets_encrypt.enabled`
- Staging mode disponível para testes sem rate limit

### 🐛 Correções e melhorias
- VirtualHost middleware refatorado com logging e statusRecorder
- Carregamento de config.json expandido (HTTPS port, SPA, Gzip, Dashboard agora respeitam o arquivo)
- Banner de startup atualizado com status das novas features
- Notificação de shutdown graceful via Telegram/Discord
- Basic auth aplicado antes do logging middleware

### ⚙️ Novo config.json — campos adicionados
```json
{
  "basic_auth": { "enabled": false, "username": "admin", "password": "...", "realm": "brhttp" },
  "vhost_log_dir": "./logs/vhosts",
  "scheduler": {
    "enabled": true,
    "jobs": [
      { "name": "backup", "cron": "0 2 * * *", "command": "bash", "args": ["./backup.sh"], "timeout_seconds": 300, "on_error": "notify", "enabled": true }
    ]
  },
  "notifications": {
    "telegram": { "enabled": true, "bot_token": "TOKEN", "chat_ids": ["CHAT_ID"] },
    "discord":  { "enabled": true, "webhook_urls": ["https://discord.com/api/webhooks/..."] }
  },
  "lets_encrypt": { "enabled": false, "domains": ["meusite.com"], "email": "seu@email.com", "staging": true }
}
```

### 📦 Binários disponíveis
| Plataforma | Arquivo |
|---|---|
| Linux x86_64 | `brhttp-linux-amd64` |
| Linux ARM64 | `brhttp-linux-arm64` |
| Linux ARM | `brhttp-linux-arm` |
| macOS Intel | `brhttp-darwin-amd64` |
| macOS Apple Silicon | `brhttp-darwin-arm64` |
| Windows x64 | `brhttp-windows-amd64.exe` |
| Windows ARM64 | `brhttp-windows-arm64.exe` |

NOTES

ok "Release notes geradas"

# ── Git tag ───────────────────────────────────────────────────────────────────
info "Criando tag Git ${TAG}..."

# Garante que estamos no branch certo
BRANCH=$(git rev-parse --abbrev-ref HEAD)
info "Branch atual: ${BRANCH}"

# Commit automático se houver mudanças
if ! git diff --quiet HEAD 2>/dev/null; then
  warn "Existem mudanças não commitadas. Commitando automaticamente..."
  git add -A
  git commit -m "chore: bump version to ${VERSION}

- Add native Cron Scheduler with cron syntax parser
- Add Telegram & Discord notifications (multi-chat/webhook)
- Add per-virtual-host log files
- Add global Basic Auth middleware
- Add Let's Encrypt ACME base structure
- Update dashboard: Scheduler tab with history, Notifications tab
- Add API endpoints: /scheduler/*, /notifications/test
- Fix config.json loading for HTTPS port, SPA, Gzip, Dashboard flags
- Update banner with new features status
"
fi

# Remove tag existente se houver
if git tag -l | grep -q "^${TAG}$"; then
  warn "Tag ${TAG} já existe — removendo..."
  git tag -d "${TAG}"
  git push origin ":refs/tags/${TAG}" 2>/dev/null || true
fi

git tag -a "${TAG}" -m "brhttp ${TAG}

Scheduler nativo, notificações Telegram/Discord, log por VHost, Basic Auth"
ok "Tag ${TAG} criada"

# ── Git push ──────────────────────────────────────────────────────────────────
info "Fazendo push para origin..."
git push origin "${BRANCH}" --follow-tags
ok "Push concluído"

# ── GitHub Release ────────────────────────────────────────────────────────────
if command -v gh >/dev/null 2>&1; then
  info "Criando release no GitHub..."
  gh release create "${TAG}" \
    --repo "${REPO}" \
    --title "brhttp ${TAG} — Scheduler · Telegram · Discord · VHost Logs" \
    --notes-file "${NOTES_FILE}" \
    ${RELEASE_DIR}/${BINARY}-* \
    ${RELEASE_DIR}/SHA256SUMS.txt

  ok "Release ${TAG} publicado em https://github.com/${REPO}/releases/tag/${TAG}"
else
  warn "gh CLI não encontrado. Para publicar o release manualmente:"
  echo
  echo "  1. Instale: https://cli.github.com/"
  echo "  2. Execute: gh auth login"
  echo "  3. Execute: gh release create ${TAG} \\"
  echo "       --repo ${REPO} \\"
  echo "       --title 'brhttp ${TAG}' \\"
  echo "       --notes-file ${NOTES_FILE} \\"
  echo "       ${RELEASE_DIR}/${BINARY}-* ${RELEASE_DIR}/SHA256SUMS.txt"
fi

# ── Resumo ────────────────────────────────────────────────────────────────────
echo
echo -e "${GREEN}════════════════════════════════════════${NC}"
echo -e "${GREEN}  brhttp ${TAG} — release concluído!${NC}"
echo -e "${GREEN}════════════════════════════════════════${NC}"
echo
echo "  📦 Binários em: ${RELEASE_DIR}/"
ls -lh "${RELEASE_DIR}/${BINARY}"-* 2>/dev/null | awk '{print "     " $5 "  " $9}'
echo
echo "  🔑 SHA256SUMS: ${RELEASE_DIR}/SHA256SUMS.txt"
echo "  📋 Notes:      ${NOTES_FILE}"
echo