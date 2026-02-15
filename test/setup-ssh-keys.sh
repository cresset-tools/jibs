#!/bin/bash
# Generate SSH keys for testing

set -e

cd "$(dirname "$0")"

SSH_KEY_DIR="ssh-keys"
mkdir -p "$SSH_KEY_DIR"

if [ ! -f "$SSH_KEY_DIR/id_ed25519" ]; then
    echo "Generating test SSH key..."
    ssh-keygen -t ed25519 -f "$SSH_KEY_DIR/id_ed25519" -N "" -C "jibs-test-key"
    # The public key is what gets mounted as authorized_keys
    cp "$SSH_KEY_DIR/id_ed25519.pub" "$SSH_KEY_DIR/authorized_keys"
    echo "SSH keys generated in $SSH_KEY_DIR/"
else
    echo "SSH keys already exist in $SSH_KEY_DIR/"
fi

echo ""
echo "Private key: $SSH_KEY_DIR/id_ed25519"
echo "Public key:  $SSH_KEY_DIR/id_ed25519.pub"
