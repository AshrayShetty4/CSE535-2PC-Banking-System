package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

type clusterCfg struct {
	StartID int      `json:"start_id"`
	EndID   int      `json:"end_id"`
	Nodes   []string `json:"nodes"`
}

type config struct {
	Clusters []clusterCfg `json:"clusters"`
	TotalIDs int          `json:"total_ids,omitempty"`
}

func autoAssignRanges(c *config) {
	if len(c.Clusters) == 0 {
		return
	}
	needAssign := false
	for _, cl := range c.Clusters {
		if cl.StartID == 0 && cl.EndID == 0 {
			needAssign = true
			break
		}
	}
	if !needAssign {
		return
	}
	total := c.TotalIDs
	if total == 0 {
		total = 9000
	}
	span := total / len(c.Clusters)
	rem := total % len(c.Clusters)
	cur := 1
	for i := range c.Clusters {
		size := span
		if i == len(c.Clusters)-1 {
			size += rem
		}
		c.Clusters[i].StartID = cur
		c.Clusters[i].EndID = cur + size - 1
		cur += size
	}
}

func main() {
	cfgPath := flag.String("config", "config/cluster.json", "path to cluster config")
	outPath := flag.String("out", "config/redistribution_map.json", "output path for id->cluster map")
	flag.Parse()

	data, err := os.ReadFile(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read cluster config: %v\n", err)
		os.Exit(1)
	}
	var cfg config
	if err := json.Unmarshal(data, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "parse cluster config: %v\n", err)
		os.Exit(1)
	}
	autoAssignRanges(&cfg)
	if len(cfg.Clusters) == 0 {
		fmt.Fprintln(os.Stderr, "no clusters defined in config")
		os.Exit(1)
	}

	mapping := make(map[string]int)
	for idx, cl := range cfg.Clusters {
		for id := cl.StartID; id <= cl.EndID; id++ {
			mapping[strconv.Itoa(id)] = idx
		}
	}

	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "create output dir: %v\n", err)
		os.Exit(1)
	}
	out, err := json.MarshalIndent(mapping, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal map: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*outPath, out, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write output: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Wrote redistribution map for %d accounts to %s\n", len(mapping), *outPath)
}
