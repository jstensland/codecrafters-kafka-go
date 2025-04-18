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
	mg.Deps(Test) // fix this first
	mg.Deps(Lint) // tidy up
	// TODO: make sure you can build binary with flags you want
	return nil
}

// Lint files
func Lint() error {
	fmt.Println("Linting...")
	return sh.RunV("golangci-lint", "run", "./...")
}

// Lint one file. Intended for aider.
func LintFile(filePath string) error {
	dirPath := filepath.Dir(filePath)
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

func Coverage() error {
	mg.Deps(Test)
	err := sh.RunV("go", "tool", "cover", "-html=build/coverage.out", "-o", "build/coverage.html")
	if err != nil {
		return fmt.Errorf("Error generating coverage report: %w", err)
	}
	cmd := exec.Command("open", "build/coverage.html")
	return cmd.Run()
}

// Build and run
func Run() error {
	return sh.RunV("go", "run", "app/main.go")
}

// Clean up after yourself
func Clean() {
	fmt.Println("Cleaning...")
	os.RemoveAll("build")
}

func setup() error {
	return os.MkdirAll("build", 0755)
}
