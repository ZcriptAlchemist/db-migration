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

	// ✅ Step 2: Clean the destination database (instead of dropping it)
	if err := cleanDestinationDB(); err != nil {
		log.Fatalf("❌ Failed to clean destination database: %v", err)
	}

	// ✅ Step 3: Ensure the Backup Directory is Fresh
	if err := resetBackupDir(dumpDir); err != nil {
		log.Fatalf("❌ Failed to reset backup directory: %v", err)
	}

	// ✅ Step 4: Dump Schema
	if err := dumpSchema(dumpDir); err != nil {
		log.Fatalf("❌ Schema dump failed: %v", err)
	}

	// ✅ Step 5: Restore Schema
	if err := restoreSchema(dumpDir); err != nil {
		log.Fatalf("❌ Schema restoration failed: %v", err)
	}

	// ✅ Step 6: Disable Constraints
	if err := disableConstraints(); err != nil {
		log.Fatalf("❌ Error disabling constraints: %v", err)
	}

	// ✅ Step 7: Dump Data
	if err := dumpData(dumpDir); err != nil {
		log.Fatalf("❌ Data dump failed: %v", err)
	}

	// ✅ Step 8: Restore Data
	if err := restoreDataWithTransaction(dumpDir); err != nil {
		log.Fatalf("❌ Data restoration failed: %v", err)
	}

	// ✅ Step 9: Re-enable Constraints
	if err := enableConstraints(); err != nil {
		log.Fatalf("❌ Error enabling constraints: %v", err)
	}

	log.Println("✅ Database migration completed successfully!")
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

// ✅ Step 2: Clean the Destination Database.
func cleanDestinationDB() error {
	log.Println("🗑️ Cleaning the destination database...")

	cleanSQL := `
	DO $$ DECLARE
		r RECORD;
	BEGIN
		EXECUTE 'SET session_replication_role = ''replica'';';
		FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
			EXECUTE 'DROP TABLE IF EXISTS public.' || r.tablename || ' CASCADE;';
		END LOOP;
		EXECUTE 'SET session_replication_role = ''origin'';';
	END $$;
	`

	cmd := exec.Command("psql", destinationDB, "-c", cleanSQL)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// ✅ Step 3: Ensure Backup Directory is Fresh
func resetBackupDir(dumpDir string) error {
	log.Println("🗑️ Removing existing backup directory...")
	if err := os.RemoveAll(dumpDir); err != nil {
		return fmt.Errorf("failed to remove backup directory: %v", err)
	}

	log.Println("📁 Creating a new backup directory...")
	return os.MkdirAll(dumpDir, 0755)
}

// ✅ Step 4: Dump Schema
func dumpSchema(dumpDir string) error {
	log.Println("📦 Dumping schema only...")
	cmd := exec.Command("pg_dump", "--format=directory", "--no-owner", "--no-acl", "--schema-only", "--dbname="+sourceDB, "--file="+dumpDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// ✅ Step 5: Restore Schema
func restoreSchema(dumpDir string) error {
	log.Println("🛠️ Restoring schema...")
	restoreCmd := exec.Command("pg_restore", "--clean", "--if-exists", "--jobs="+jobs, "--no-owner", "--no-acl", "--schema-only", "--dbname="+destinationDB, dumpDir)
	restoreCmd.Stdout = os.Stdout
	restoreCmd.Stderr = os.Stderr
	return restoreCmd.Run()
}

// ✅ Step 6: Disable Constraints
func disableConstraints() error {
	log.Println("⏸️ Disabling foreign key constraints...")
	cmd := exec.Command("psql", destinationDB, "-c", "SET session_replication_role = 'replica';")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// ✅ Step 7: Dump Data
func dumpData(dumpDir string) error {
	log.Println("📦 Dumping data only...")
	cmd := exec.Command("pg_dump", "--format=directory", "--no-owner", "--no-acl", "--data-only", "--jobs="+jobs, "--dbname="+sourceDB, "--file="+dumpDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// ✅ Step 8: Restore Data
func restoreDataWithTransaction(dumpDir string) error {
	log.Println("🛠️ Restoring data within a transaction...")
	restoreCmd := exec.Command("pg_restore", "--disable-triggers", "--jobs="+jobs, "--no-owner", "--no-acl", "--data-only", "--dbname="+destinationDB, dumpDir)
	restoreCmd.Stdout = os.Stdout
	restoreCmd.Stderr = os.Stderr
	if err := restoreCmd.Run(); err != nil {
		return fmt.Errorf("data restore failed: %v", err)
	}
	return nil
}

// ✅ Step 9: Re-enable Constraints
func enableConstraints() error {
	log.Println("✅ Re-enabling foreign key constraints...")
	cmd := exec.Command("psql", destinationDB, "-c", "SET session_replication_role = 'origin';")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
