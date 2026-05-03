//go:build toml

package config

import "github.com/BurntSushi/toml"

// parseTOML decodes TOML into v. Compiled in only with `-tags toml`.
func parseTOML(data []byte, v any) error {
	return toml.Unmarshal(data, v)
}
