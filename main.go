package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	_ "github.com/lib/pq"
)

const (
	sourceDB      = "your_source_db_connection_string"
	destinationDB = "your_destination_db_connection_string"
	jobs          = "4" // Number of parallel jobs for performance
)

func main() {
	// Get working directory for backups
	workingDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("❌ Unable to get current working directory: %v", err)
	}
	dumpDir := filepath.Join(workingDir, "backup_dir")

	log.Printf("Using %s as the backup directory...\n", dumpDir)

	// ✅ Step 1: Test Database Connections
	if !testDBConnection(sourceDB, "Source") || !testDBConnection(destinationDB, "Destination") {
		log.Fatal("❌ Database connection test failed. Migration aborted.")
	}

	// ✅ Step 2: Ensure the Backup Directory is Fresh
	if err := resetBackupDir(dumpDir); err != nil {
		log.Fatalf("❌ Failed to reset backup directory: %v", err)
	}

	// ✅ Step 3: Dump Data
	if err := dumpData(dumpDir); err != nil {
		log.Fatalf("❌ Data dump failed: %v", err)
	}

	// ✅ Step 4: Restore Data
	if err := restoreData(dumpDir); err != nil {
		log.Fatalf("❌ Data restoration failed: %v", err)
	}

	log.Println("✅ Database data migration completed successfully!")
}

// ✅ Step 1: Test Database Connection
func testDBConnection(connStr, dbType string) bool {
	log.Printf("🔄 Testing %s database connection...", dbType)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Printf("❌ %s Database connection error: %v", dbType, err)
		return false
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Printf("❌ %s Database is unreachable: %v", dbType, err)
		return false
	}

	log.Printf("✅ %s Database connection successful!", dbType)
	return true
}

// ✅ Step 2: Ensure Backup Directory is Fresh
func resetBackupDir(dumpDir string) error {
	log.Println("🗑️ Removing existing backup directory...")
	if err := os.RemoveAll(dumpDir); err != nil {
		return fmt.Errorf("failed to remove backup directory: %v", err)
	}

	log.Println("📁 Creating a new backup directory...")
	return os.MkdirAll(dumpDir, 0755)
}

// ✅ Step 3: Dump Data
func dumpData(dumpDir string) error {
	log.Println("📦 Dumping data only...")
	cmd := exec.Command("pg_dump", "--format=directory", "--no-owner", "--no-acl", "--data-only", "--jobs="+jobs, "--dbname="+sourceDB, "--file="+dumpDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// ✅ Step 4: Restore Data
func restoreData(dumpDir string) error {
	log.Println("🛠️ Restoring data...")
	restoreCmd := exec.Command("pg_restore", "--jobs="+jobs, "--no-owner", "--no-acl", "--data-only", "--dbname="+destinationDB, dumpDir)
	restoreCmd.Stdout = os.Stdout
	restoreCmd.Stderr = os.Stderr
	return restoreCmd.Run()
}
