find . -path ./target -prune -o \
    \( -name "*.rs" -a ! -name "broker_service.rs" -o -name "*.toml" -o -name "*.proto" \) \
    -type f -exec sh -c 'echo "File: {}" && cat {} && echo ""' \;
