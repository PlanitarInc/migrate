package migrate

import (
	"io/ioutil"
	"testing"
)

// Add Driver URLs here to test basic Up, Down, .. functions.
var driverUrls = []string{
	"postgres://localhost/migratetest?sslmode=disable",
}

func TestCreate(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		opts := &Options{Url: driverUrl, Path: tmpdir}

		if _, err := Create(opts, "test_migration"); err != nil {
			t.Fatal(err)
		}
		if _, err := Create(opts, "another migration"); err != nil {
			t.Fatal(err)
		}

		files, err := ioutil.ReadDir(tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != 4 {
			t.Fatal("Expected 2 new files, got", len(files))
		}
		expectFiles := []string{
			"0001_test_migration.up.sql", "0001_test_migration.down.sql",
			"0002_another_migration.up.sql", "0002_another_migration.down.sql",
		}
		foundCounter := 0
		for _, expectFile := range expectFiles {
			for _, file := range files {
				if expectFile == file.Name() {
					foundCounter += 1
					break
				}
			}
		}
		if foundCounter != len(expectFiles) {
			t.Error("not all expected files have been found")
		}
	}
}

func TestReset(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		opts := &Options{Url: driverUrl, Path: tmpdir}

		Create(opts, "migration1")
		Create(opts, "migration2")

		errs, ok := ResetSync(opts)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(opts)
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}
	}
}

func TestDown(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		opts := &Options{Url: driverUrl, Path: tmpdir}

		Create(opts, "migration1")
		Create(opts, "migration2")

		errs, ok := ResetSync(opts)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(opts)
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}

		errs, ok = DownSync(opts)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(opts)
		if err != nil {
			t.Fatal(err)
		}
		if version != 0 {
			t.Fatalf("Expected version 0, got %v", version)
		}
	}
}

func TestUp(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		opts := &Options{Url: driverUrl, Path: tmpdir}

		Create(opts, "migration1")
		Create(opts, "migration2")

		errs, ok := DownSync(opts)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(opts)
		if err != nil {
			t.Fatal(err)
		}
		if version != 0 {
			t.Fatalf("Expected version 0, got %v", version)
		}

		errs, ok = UpSync(opts)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(opts)
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}
	}
}

func TestRedo(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		opts := &Options{Url: driverUrl, Path: tmpdir}

		Create(opts, "migration1")
		Create(opts, "migration2")

		errs, ok := ResetSync(opts)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(opts)
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}

		errs, ok = RedoSync(opts)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(opts)
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}
	}
}

func TestMigrate(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		opts := &Options{Url: driverUrl, Path: tmpdir}

		Create(opts, "migration1")
		Create(opts, "migration2")

		errs, ok := ResetSync(opts)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(opts)
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}

		errs, ok = MigrateSync(opts, -2)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(opts)
		if err != nil {
			t.Fatal(err)
		}
		if version != 0 {
			t.Fatalf("Expected version 0, got %v", version)
		}

		errs, ok = MigrateSync(opts, +1)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(opts)
		if err != nil {
			t.Fatal(err)
		}
		if version != 1 {
			t.Fatalf("Expected version 1, got %v", version)
		}
	}
}
