#!/bin/sh
# Conduit installer — POSIX sh.
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/jayhere1/conduit/main/scripts/install.sh | sh
#
# Environment overrides:
#   CONDUIT_REPO        owner/repo on GitHub (default: jayhere1/conduit)
#   CONDUIT_VERSION     specific version tag like v0.3.0 (default: latest)
#   CONDUIT_INSTALL_DIR install destination (default: $HOME/.local/bin)
set -eu

# ---- configuration ---------------------------------------------------------
# NOTE: Update CONDUIT_REPO below if the canonical repository ever moves.
CONDUIT_REPO="${CONDUIT_REPO:-jayhere1/conduit}"
CONDUIT_VERSION="${CONDUIT_VERSION:-latest}"
CONDUIT_INSTALL_DIR="${CONDUIT_INSTALL_DIR:-$HOME/.local/bin}"
BIN_NAME="conduit"

# ---- helpers ---------------------------------------------------------------
log()  { printf '%s\n' "==> $*"; }
warn() { printf '%s\n' "warning: $*" >&2; }
die()  { printf '%s\n' "error: $*" >&2; exit 1; }

need() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

TMPDIR_INSTALL=""
cleanup() {
    if [ -n "$TMPDIR_INSTALL" ] && [ -d "$TMPDIR_INSTALL" ]; then
        rm -rf "$TMPDIR_INSTALL"
    fi
}
trap cleanup EXIT INT HUP TERM

# ---- prerequisites ---------------------------------------------------------
need tar
need uname
need mkdir
need chmod

# Need either curl or wget
DOWNLOADER=""
if command -v curl >/dev/null 2>&1; then
    DOWNLOADER="curl"
elif command -v wget >/dev/null 2>&1; then
    DOWNLOADER="wget"
else
    die "neither curl nor wget is installed"
fi

http_get() {
    # http_get URL OUTFILE
    url="$1"
    out="$2"
    if [ "$DOWNLOADER" = "curl" ]; then
        curl -fsSL --retry 3 --retry-delay 2 -o "$out" "$url"
    else
        wget -q --tries=3 -O "$out" "$url"
    fi
}

http_get_stdout() {
    url="$1"
    if [ "$DOWNLOADER" = "curl" ]; then
        curl -fsSL --retry 3 --retry-delay 2 "$url"
    else
        wget -qO- "$url"
    fi
}

# ---- detect platform -------------------------------------------------------
detect_os() {
    os_raw="$(uname -s)"
    case "$os_raw" in
        Linux)  echo "linux" ;;
        Darwin) echo "macos" ;;
        *) die "unsupported OS: $os_raw (supported: Linux, Darwin)" ;;
    esac
}

detect_arch() {
    arch_raw="$(uname -m)"
    case "$arch_raw" in
        x86_64|amd64)      echo "x86_64" ;;
        aarch64|arm64)     echo "aarch64" ;;
        *) die "unsupported architecture: $arch_raw (supported: x86_64, aarch64)" ;;
    esac
}

OS="$(detect_os)"
ARCH="$(detect_arch)"
PLATFORM="${ARCH}-${OS}"
log "detected platform: $PLATFORM"

# ---- resolve version -------------------------------------------------------
resolve_version() {
    if [ "$CONDUIT_VERSION" != "latest" ]; then
        echo "$CONDUIT_VERSION"
        return
    fi
    api_url="https://api.github.com/repos/${CONDUIT_REPO}/releases/latest"
    # Parse "tag_name": "vX.Y.Z" without requiring jq.
    tag="$(http_get_stdout "$api_url" \
        | grep -E '"tag_name"[[:space:]]*:' \
        | head -n1 \
        | sed -E 's/.*"tag_name"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')"
    if [ -z "$tag" ]; then
        die "could not resolve latest release tag from $api_url"
    fi
    echo "$tag"
}

TAG="$(resolve_version)"
VERSION="${TAG#v}"
log "installing $BIN_NAME $TAG"

# ---- download --------------------------------------------------------------
ARCHIVE="${BIN_NAME}-${VERSION}-${PLATFORM}.tar.gz"
CHECKSUM="${ARCHIVE}.sha256"
BASE_URL="https://github.com/${CONDUIT_REPO}/releases/download/${TAG}"
ARCHIVE_URL="${BASE_URL}/${ARCHIVE}"
CHECKSUM_URL="${BASE_URL}/${CHECKSUM}"

TMPDIR_INSTALL="$(mktemp -d 2>/dev/null || mktemp -d -t conduit-install)"
[ -n "$TMPDIR_INSTALL" ] || die "could not create temp directory"

log "downloading $ARCHIVE_URL"
http_get "$ARCHIVE_URL" "$TMPDIR_INSTALL/$ARCHIVE" \
    || die "failed to download $ARCHIVE_URL (does the asset exist for $PLATFORM?)"

# ---- verify checksum (best effort) ----------------------------------------
SUMTOOL=""
if command -v sha256sum >/dev/null 2>&1; then
    SUMTOOL="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
    SUMTOOL="shasum -a 256"
fi

if [ -n "$SUMTOOL" ]; then
    if http_get "$CHECKSUM_URL" "$TMPDIR_INSTALL/$CHECKSUM" 2>/dev/null; then
        log "verifying sha256 checksum"
        # Recompute and compare hashes (avoid `cd` so we stay portable)
        expected="$(awk '{print $1}' "$TMPDIR_INSTALL/$CHECKSUM")"
        actual="$($SUMTOOL "$TMPDIR_INSTALL/$ARCHIVE" | awk '{print $1}')"
        if [ "$expected" != "$actual" ]; then
            die "checksum mismatch: expected $expected, got $actual"
        fi
        log "checksum OK"
    else
        warn "no published checksum for $ARCHIVE — skipping verification"
        warn "(follow-up: publish .sha256 alongside release assets)"
    fi
else
    warn "neither sha256sum nor shasum found — skipping checksum verification"
fi

# ---- extract ---------------------------------------------------------------
log "extracting archive"
tar -xzf "$TMPDIR_INSTALL/$ARCHIVE" -C "$TMPDIR_INSTALL" \
    || die "failed to extract $ARCHIVE"

# Find the binary inside the archive (release layout: {name}-{version}-{platform}/conduit)
SRC_BIN="$(find "$TMPDIR_INSTALL" -type f -name "$BIN_NAME" -perm -u+x 2>/dev/null | head -n1)"
if [ -z "$SRC_BIN" ]; then
    # Fallback: any file named conduit
    SRC_BIN="$(find "$TMPDIR_INSTALL" -type f -name "$BIN_NAME" 2>/dev/null | head -n1)"
fi
[ -n "$SRC_BIN" ] || die "could not find '$BIN_NAME' binary in extracted archive"

# ---- install ---------------------------------------------------------------
mkdir -p "$CONDUIT_INSTALL_DIR" || die "failed to create $CONDUIT_INSTALL_DIR"
DEST="$CONDUIT_INSTALL_DIR/$BIN_NAME"

if [ -e "$DEST" ]; then
    log "replacing existing binary at $DEST"
    rm -f "$DEST" || die "could not remove existing $DEST (insufficient permissions?)"
fi

# Use cp + chmod rather than `install` (not in pristine POSIX environments)
cp "$SRC_BIN" "$DEST" || die "failed to copy binary to $DEST"
chmod 0755 "$DEST" || die "failed to chmod $DEST"

# ---- verify ----------------------------------------------------------------
log "installed to $DEST"
if "$DEST" --version >/dev/null 2>&1; then
    version_line="$("$DEST" --version 2>&1 | head -n1)"
    log "verified: $version_line"
else
    warn "could not run '$DEST --version'; the binary may be incompatible with this system"
fi

# ---- PATH hint -------------------------------------------------------------
case ":${PATH:-}:" in
    *":$CONDUIT_INSTALL_DIR:"*)
        log "done. '$BIN_NAME' is on your PATH."
        ;;
    *)
        printf '\n'
        log "done — but $CONDUIT_INSTALL_DIR is NOT on your PATH."
        printf '\n'
        printf '  Add this to your shell rc (~/.bashrc, ~/.zshrc, ~/.profile):\n\n'
        printf '    export PATH="%s:$PATH"\n\n' "$CONDUIT_INSTALL_DIR"
        printf '  Then reopen your shell or run: source ~/.bashrc\n\n'
        ;;
esac
