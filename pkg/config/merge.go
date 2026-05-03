package config

// LBFlags holds CLI-supplied values for the load balancer. Empty strings and
// nil slices mean "not set by user" — the merge logic preserves the file
// value in that case. This relies on the convention that we set CLI flag
// defaults to "" (or nil) rather than the actual default, so the absence of
// a flag is distinguishable from an explicit empty value.
type LBFlags struct {
	Listen     string
	Strategy   string
	GossipAddr string
	NodeID     string
	SeedNodes  []string
	LogLevel   string
	LogFormat  string
}

// MergeLB applies CLI flags on top of a file-loaded LBConfig.
// Empty/nil fields in flags do not overwrite existing config values.
func MergeLB(cfg LBConfig, flags LBFlags) LBConfig {
	if flags.Listen != "" {
		cfg.Listen = flags.Listen
	}
	if flags.Strategy != "" {
		cfg.Strategy = flags.Strategy
	}
	if flags.GossipAddr != "" {
		cfg.GossipAddr = flags.GossipAddr
	}
	if flags.NodeID != "" {
		cfg.NodeID = flags.NodeID
	}
	if len(flags.SeedNodes) > 0 {
		cfg.SeedNodes = flags.SeedNodes
	}
	if flags.LogLevel != "" {
		cfg.Logging.Level = flags.LogLevel
	}
	if flags.LogFormat != "" {
		cfg.Logging.Format = flags.LogFormat
	}
	return cfg
}

// NodeFlags holds CLI-supplied values for backend nodes.
type NodeFlags struct {
	ID          string
	GossipAddr  string
	ServiceAddr string
	SeedNodes   []string
	LogLevel    string
	LogFormat   string
}

// MergeNode applies CLI flags on top of a file-loaded NodeConfig.
func MergeNode(cfg NodeConfig, flags NodeFlags) NodeConfig {
	if flags.ID != "" {
		cfg.ID = flags.ID
	}
	if flags.GossipAddr != "" {
		cfg.GossipAddr = flags.GossipAddr
	}
	if flags.ServiceAddr != "" {
		cfg.ServiceAddr = flags.ServiceAddr
	}
	if len(flags.SeedNodes) > 0 {
		cfg.SeedNodes = flags.SeedNodes
	}
	if flags.LogLevel != "" {
		cfg.Logging.Level = flags.LogLevel
	}
	if flags.LogFormat != "" {
		cfg.Logging.Format = flags.LogFormat
	}
	return cfg
}
