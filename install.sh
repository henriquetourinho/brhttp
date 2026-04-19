#!/usr/bin/env sh
# brhttp installer — https://github.com/henriquetourinho/brhttp
# Uso: curl -fsSL https://raw.githubusercontent.com/henriquetourinho/brhttp/main/install.sh | sh

set -e

REPO="henriquetourinho/brhttp"
BINARY="brhttp"
INSTALL_DIR="/usr/local/bin"

# ── Detecta OS e arch ─────────────────────────────────────────────────────────
OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
  Linux)
    case "$ARCH" in
      x86_64)  SUFFIX="linux-amd64" ;;
      aarch64) SUFFIX="linux-arm64" ;;
      armv7*)  SUFFIX="linux-armv7" ;;
      *)
        echo "❌ Arquitetura não suportada: $ARCH"
        exit 1
        ;;
    esac
    ;;
  Darwin)
    case "$ARCH" in
      x86_64)  SUFFIX="macos-intel" ;;
      arm64)   SUFFIX="macos-apple-silicon" ;;
      *)
        echo "❌ Arquitetura não suportada: $ARCH"
        exit 1
        ;;
    esac
    ;;
  *)
    echo "❌ Sistema operacional não suportado: $OS"
    echo "   Para Windows, baixe manualmente em: https://github.com/${REPO}/releases"
    exit 1
    ;;
esac

# ── Obtém a versão mais recente ───────────────────────────────────────────────
echo "🔍 Verificando a versão mais recente..."

if command -v curl >/dev/null 2>&1; then
  LATEST=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')
elif command -v wget >/dev/null 2>&1; then
  LATEST=$(wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')
else
  echo "❌ curl ou wget são necessários para instalar o brhttp."
  exit 1
fi

if [ -z "$LATEST" ]; then
  echo "❌ Não foi possível determinar a versão mais recente."
  exit 1
fi

echo "📦 Versão encontrada: $LATEST"

# ── Download ──────────────────────────────────────────────────────────────────
FILENAME="${BINARY}-${SUFFIX}.tar.gz"
URL="https://github.com/${REPO}/releases/download/${LATEST}/${FILENAME}"
TMP_DIR="$(mktemp -d)"

echo "⬇️  Baixando ${FILENAME}..."

if command -v curl >/dev/null 2>&1; then
  curl -fsSL "$URL" -o "${TMP_DIR}/${FILENAME}"
else
  wget -q "$URL" -O "${TMP_DIR}/${FILENAME}"
fi

# ── Extrai e instala ──────────────────────────────────────────────────────────
echo "📂 Extraindo..."
tar -xzf "${TMP_DIR}/${FILENAME}" -C "${TMP_DIR}"

# Determina se precisa de sudo
if [ -w "$INSTALL_DIR" ]; then
  SUDO=""
else
  SUDO="sudo"
  echo "🔐 Permissão de escrita necessária em ${INSTALL_DIR} — solicitando sudo..."
fi

$SUDO mv "${TMP_DIR}/${BINARY}-${SUFFIX}" "${INSTALL_DIR}/${BINARY}"
$SUDO chmod +x "${INSTALL_DIR}/${BINARY}"

# Limpeza
rm -rf "$TMP_DIR"

# ── Verifica instalação ───────────────────────────────────────────────────────
if command -v brhttp >/dev/null 2>&1; then
  echo ""
  echo "✅ brhttp ${LATEST} instalado com sucesso em ${INSTALL_DIR}/${BINARY}"
  echo ""
  echo "   Uso rápido:"
  echo "     brhttp                          # serve ./www na porta 5571"
  echo "     brhttp --dir=dist --port=3000   # serve ./dist na porta 3000"
  echo "     brhttp --https --dashboard      # + HTTPS e dashboard"
  echo "     brhttp --help                   # todas as opções"
  echo ""
else
  echo "⚠️  Instalação concluída, mas '${BINARY}' não encontrado no PATH."
  echo "   Adicione ${INSTALL_DIR} ao seu PATH ou mova o binário manualmente."
fi
