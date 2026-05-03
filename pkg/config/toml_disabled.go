//go:build !toml

package config

import "fmt"

// parseTOML returns an instructive error in the default build. To enable
// TOML support, build with `-tags toml` (which pulls in BurntSushi/toml).
func parseTOML(_ []byte, _ any) error {
	return fmt.Errorf("TOML support not compiled in (rebuild with `-tags toml`)")
}
