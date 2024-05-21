find . -path ./target -prune -o \( -name "*.rs" -o -name "*.toml" -o -name "*.proto" \) -type f -exec sh -c 'echo "File: {}" && cat {} && echo ""' \;
