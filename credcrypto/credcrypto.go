// Package credcrypto provides symmetric encryption + masking for runtime-managed
// service credentials (third-party API keys stored in each service's DB so they
// can be rotated from the admin panel without a redeploy).
//
// Scheme: AES-256-GCM. The 32-byte key is derived as SHA-256(secret) where
// secret = CRED_ENCRYPTION_KEY (preferred) or SECRET_KEY (dev fallback, so the
// feature works before a dedicated key is provisioned). Ciphertext format:
// "c1:" + base64(nonce || sealed). Stdlib + viper only — safe to import from
// every service without pulling new module deps.
package credcrypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	"github.com/spf13/viper"
)

const version = "c1:"

// ErrNoKey is returned when neither CRED_ENCRYPTION_KEY nor SECRET_KEY is set.
var ErrNoKey = errors.New("credcrypto: no encryption key (set CRED_ENCRYPTION_KEY)")

// deriveKey returns a 32-byte AES key from the configured secret. Using SHA-256
// lets the operator use a human-typed secret of any length.
func deriveKey() ([]byte, error) {
	secret := viper.GetString("CRED_ENCRYPTION_KEY")
	if secret == "" {
		// Dev fallback: reuse the JWT secret so the feature works out of the
		// box. Prod should set a dedicated CRED_ENCRYPTION_KEY.
		secret = viper.GetString("SECRET_KEY")
	}
	if secret == "" {
		return nil, ErrNoKey
	}
	sum := sha256.Sum256([]byte(secret))
	return sum[:], nil
}

// Available reports whether an encryption key is configured. Handlers use this
// to fail a write cleanly instead of panicking.
func Available() bool {
	_, err := deriveKey()
	return err == nil
}

// Encrypt seals plaintext with AES-256-GCM. Output: "c1:" + base64(nonce||sealed).
func Encrypt(plaintext string) (string, error) {
	key, err := deriveKey()
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("credcrypto: cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("credcrypto: gcm: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("credcrypto: nonce: %w", err)
	}
	sealed := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return version + base64.StdEncoding.EncodeToString(sealed), nil
}

// Decrypt reverses Encrypt. Errors on tampered/short input or wrong key.
func Decrypt(ciphertext string) (string, error) {
	if len(ciphertext) < len(version) || ciphertext[:len(version)] != version {
		return "", errors.New("credcrypto: bad version prefix")
	}
	key, err := deriveKey()
	if err != nil {
		return "", err
	}
	raw, err := base64.StdEncoding.DecodeString(ciphertext[len(version):])
	if err != nil {
		return "", fmt.Errorf("credcrypto: base64: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("credcrypto: cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("credcrypto: gcm: %w", err)
	}
	ns := gcm.NonceSize()
	if len(raw) < ns {
		return "", errors.New("credcrypto: ciphertext too short")
	}
	nonce, sealed := raw[:ns], raw[ns:]
	plaintext, err := gcm.Open(nil, nonce, sealed, nil)
	if err != nil {
		return "", fmt.Errorf("credcrypto: open: %w", err)
	}
	return string(plaintext), nil
}

// Mask returns a non-reversible preview of a secret for display: last 4 chars
// revealed, the rest as dots. Short values are fully masked. Empty → "".
func Mask(value string) string {
	if value == "" {
		return ""
	}
	if len(value) <= 4 {
		return "••••"
	}
	return "••••" + value[len(value)-4:]
}
