//go:build mage
// +build mage

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/magefile/mage/mg" // mg contains helpful utility functions, like Deps
	"github.com/magefile/mage/sh" // sh contains helpers for running commands
)

// Default target to run when none is specified
// If not set, running mage will list available targets
var Default = All

// All will Do all the things one should do before pushing
func All() error {
	mg.Deps(Format) // fix formatting
	mg.Deps(Test)   // fix this first
	mg.Deps(Lint)   // tidy up
	// TODO: make sure you can build binary with flags you want
	return nil
}

// Format it all
func Format() error {
	fmt.Println("Formatting...")
	// TODO: add import handling
	return sh.RunV("gofumpt", "-w", "-extra", ".")
}

// Lint files
func Lint() error {
	fmt.Println("Linting...")
	return sh.RunV("golangci-lint", "run", "./...")
}

// Format and then lint one file. Intended for aider.
func FormatLintFile(filePath string) error {
	dirPath := filepath.Dir(filePath)
	fmt.Println("formating file first:", filePath)
	err := sh.RunV("gofumpt", "-w", "-extra", filePath)
	if err != nil {
		return fmt.Errorf("Error formatting code before linting: %w", err)
	}
	fmt.Println("Linting package in:", dirPath)
	return sh.RunV("golangci-lint", "run", dirPath)
}

// Run unit tests
func Test() error {
	fmt.Println("Unit tests...")
	err := setup()
	if err != nil {
		return fmt.Errorf("Error generating coverage report: %w", err)
	}
	return sh.RunV("go", "test", "-race", "-covermode=atomic", "-coverprofile=build/coverage.out", "./...")
}

// Run unit tests verbosely
func TestVerbose() error {
	fmt.Println("Verbose unit tests...")
	err := setup()
	if err != nil {
		return fmt.Errorf("Error generating coverage report: %w", err)
	}
	return sh.RunV("go", "test", "-race", "-covermode=atomic", "-coverprofile=build/coverage.out", "./...", "-v")
}

// compute and display coverage
func Coverage() error {
	mg.Deps(Test)
	err := sh.RunV("go", "tool", "cover", "-html=build/coverage.out", "-o", "build/coverage.html")
	if err != nil {
		return fmt.Errorf("Error generating coverage report: %w", err)
	}
	cmd := exec.Command("open", "build/coverage.html")
	return cmd.Run()
}

// go run the app
func Run() error {
	return sh.RunV("go", "run", "app/main.go")
}

// Clean remove build artifacts
func Clean() {
	fmt.Println("Cleaning...")
	os.RemoveAll("build")
}

func setup() error {
	return os.MkdirAll("build", 0o755)
}
