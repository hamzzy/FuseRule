#!/bin/bash
# Generate shell completions for fuserule CLI

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "🔧 Generating shell completions for fuserule..."

# Check if fuserule binary exists
if [ ! -f "$PROJECT_ROOT/target/release/fuserule" ]; then
    echo "⚠️  fuserule binary not found. Building release version..."
    cd "$PROJECT_ROOT"
    cargo build --release
fi

# Generate completions
echo "📝 Generating bash completion..."
mkdir -p "$PROJECT_ROOT/completions"
"$PROJECT_ROOT/target/release/fuserule" completions bash > "$PROJECT_ROOT/completions/fuserule.bash" 2>/dev/null || {
    echo "⚠️  Failed to generate bash completion"
}

echo "📝 Generating zsh completion..."
"$PROJECT_ROOT/target/release/fuserule" completions zsh > "$PROJECT_ROOT/completions/_fuserule" 2>/dev/null || {
    echo "⚠️  Failed to generate zsh completion"
}

echo "📝 Generating fish completion..."
"$PROJECT_ROOT/target/release/fuserule" completions fish > "$PROJECT_ROOT/completions/fuserule.fish" 2>/dev/null || {
    echo "⚠️  Failed to generate fish completion"
}

echo ""
echo "✅ Completions generated in $PROJECT_ROOT/completions/"
echo ""
echo "To install:"
echo "  Bash: source $PROJECT_ROOT/completions/fuserule.bash"
echo "  Zsh:  Add to ~/.zshrc: fpath=($PROJECT_ROOT/completions $fpath)"
echo "  Fish: cp $PROJECT_ROOT/completions/fuserule.fish ~/.config/fish/completions/"

