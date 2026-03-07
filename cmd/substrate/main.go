// Command substrate is the CLI entry point for the Substrate AWS emulator.
//
// Usage:
//
//	substrate [flags]
//
// Flags:
//
//	-version  print version and exit
package main

import (
	"flag"
	"fmt"
	"os"
)

// version is set at build time via -ldflags "-X main.version=v0.1.0".
var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "substrate: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("substrate", flag.ContinueOnError)
	printVersion := fs.Bool("version", false, "print version and exit")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *printVersion {
		fmt.Printf("substrate %s\n", version)
		return nil
	}

	// TODO(#12): implement server start, replay, and debug sub-commands.
	fmt.Fprintf(os.Stderr, "substrate %s — server not yet implemented\n", version)
	fmt.Fprintf(os.Stderr, "See https://github.com/scttfrdmn/substrate/issues for progress.\n")
	return nil
}
