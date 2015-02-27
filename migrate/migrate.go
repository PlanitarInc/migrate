// Package migrate is imported by other Go code.
// It is the entry point to all migration functions.
package migrate

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"

	"github.com/PlanitarInc/migrate/driver"
	"github.com/PlanitarInc/migrate/file"
	"github.com/PlanitarInc/migrate/migrate/direction"
	pipep "github.com/PlanitarInc/migrate/pipe"
)

type Options struct {
	Url      string
	Instance interface{}
	Path     string
}

// Up applies all available migrations
func Up(pipe chan interface{}, opts *Options) {
	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(opts)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	applyMigrationFiles, err := files.ToLastFrom(version)
	if err != nil {
		if err2 := d.Close(); err2 != nil {
			pipe <- err2
		}
		go pipep.Close(pipe, err)
		return
	}

	if len(applyMigrationFiles) > 0 {
		for _, f := range applyMigrationFiles {
			pipe1 := pipep.New()
			go d.Migrate(f, pipe1)
			if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
				break
			}
		}
		if err := d.Close(); err != nil {
			pipe <- err
		}
		go pipep.Close(pipe, nil)
		return
	} else {
		if err := d.Close(); err != nil {
			pipe <- err
		}
		go pipep.Close(pipe, nil)
		return
	}
}

// UpSync is synchronous version of Up
func UpSync(opts *Options) (err []error, ok bool) {
	pipe := pipep.New()
	go Up(pipe, opts)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Down rolls back all migrations
func Down(pipe chan interface{}, opts *Options) {
	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(opts)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	applyMigrationFiles, err := files.ToFirstFrom(version)
	if err != nil {
		if err2 := d.Close(); err2 != nil {
			pipe <- err2
		}
		go pipep.Close(pipe, err)
		return
	}

	if len(applyMigrationFiles) > 0 {
		for _, f := range applyMigrationFiles {
			pipe1 := pipep.New()
			go d.Migrate(f, pipe1)
			if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
				break
			}
		}
		if err2 := d.Close(); err2 != nil {
			pipe <- err2
		}
		go pipep.Close(pipe, nil)
		return
	} else {
		if err2 := d.Close(); err2 != nil {
			pipe <- err2
		}
		go pipep.Close(pipe, nil)
		return
	}
}

// DownSync is synchronous version of Down
func DownSync(opts *Options) (err []error, ok bool) {
	pipe := pipep.New()
	go Down(pipe, opts)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Redo rolls back the most recently applied migration, then runs it again.
func Redo(pipe chan interface{}, opts *Options) {
	pipe1 := pipep.New()
	go Migrate(pipe1, opts, -1)
	if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
		go pipep.Close(pipe, nil)
		return
	} else {
		go Migrate(pipe, opts, +1)
	}
}

// RedoSync is synchronous version of Redo
func RedoSync(opts *Options) (err []error, ok bool) {
	pipe := pipep.New()
	go Redo(pipe, opts)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Reset runs the down and up migration function
func Reset(pipe chan interface{}, opts *Options) {
	pipe1 := pipep.New()
	go Down(pipe1, opts)
	if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
		go pipep.Close(pipe, nil)
		return
	} else {
		go Up(pipe, opts)
	}
}

// ResetSync is synchronous version of Reset
func ResetSync(opts *Options) (err []error, ok bool) {
	pipe := pipep.New()
	go Reset(pipe, opts)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Migrate applies relative +n/-n migrations
func Migrate(pipe chan interface{}, opts *Options, relativeN int) {
	d, files, version, err := initDriverAndReadMigrationFilesAndGetVersion(opts)
	if err != nil {
		go pipep.Close(pipe, err)
		return
	}

	applyMigrationFiles, err := files.From(version, relativeN)
	if err != nil {
		if err2 := d.Close(); err2 != nil {
			pipe <- err2
		}
		go pipep.Close(pipe, err)
		return
	}

	if len(applyMigrationFiles) > 0 && relativeN != 0 {
		for _, f := range applyMigrationFiles {
			pipe1 := pipep.New()
			go d.Migrate(f, pipe1)
			if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
				break
			}
		}
		if err2 := d.Close(); err2 != nil {
			pipe <- err2
		}
		go pipep.Close(pipe, nil)
		return
	}
	if err2 := d.Close(); err2 != nil {
		pipe <- err2
	}
	go pipep.Close(pipe, nil)
	return
}

// MigrateSync is synchronous version of Migrate
func MigrateSync(opts *Options, relativeN int) (err []error, ok bool) {
	pipe := pipep.New()
	go Migrate(pipe, opts, relativeN)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Version returns the current migration version
func Version(opts *Options) (version uint64, err error) {
	d, err := driver.New(opts.Instance, opts.Url)
	if err != nil {
		return 0, err
	}
	return d.Version()
}

// Create creates new migration files on disk
func Create(opts *Options, name string) (*file.MigrationFile, error) {
	d, err := driver.New(opts.Instance, opts.Url)
	if err != nil {
		return nil, err
	}
	files, err := file.ReadMigrationFilesFromStore(fileStore, opts.Path,
		file.FilenameRegex(d.FilenameExtension()))
	if err != nil {
		return nil, err
	}

	version := uint64(0)
	if len(files) > 0 {
		lastFile := files[len(files)-1]
		version = lastFile.Version
	}
	version += 1
	versionStr := strconv.FormatUint(version, 10)

	length := 4 // TODO(mattes) check existing files and try to guess length
	if len(versionStr)%length != 0 {
		versionStr = strings.Repeat("0", length-len(versionStr)%length) + versionStr
	}

	filenamef := "%s_%s.%s.%s"
	name = strings.Replace(name, " ", "_", -1)

	mfile := &file.MigrationFile{
		Version: version,
		UpFile: &file.File{
			Path:      opts.Path,
			FileName:  fmt.Sprintf(filenamef, versionStr, name, "up", d.FilenameExtension()),
			Name:      name,
			Content:   []byte(""),
			Direction: direction.Up,
		},
		DownFile: &file.File{
			Path:      opts.Path,
			FileName:  fmt.Sprintf(filenamef, versionStr, name, "down", d.FilenameExtension()),
			Name:      name,
			Content:   []byte(""),
			Direction: direction.Down,
		},
	}

	if err := ioutil.WriteFile(path.Join(mfile.UpFile.Path, mfile.UpFile.FileName), mfile.UpFile.Content, 0644); err != nil {
		return nil, err
	}
	if err := ioutil.WriteFile(path.Join(mfile.DownFile.Path, mfile.DownFile.FileName), mfile.DownFile.Content, 0644); err != nil {
		return nil, err
	}

	return mfile, nil
}

// initDriverAndReadMigrationFilesAndGetVersion is a small helper
// function that is common to most of the migration funcs
func initDriverAndReadMigrationFilesAndGetVersion(opts *Options) (driver.Driver, *file.MigrationFiles, uint64, error) {
	d, err := driver.New(opts.Instance, opts.Url)
	if err != nil {
		return nil, nil, 0, err
	}
	files, err := file.ReadMigrationFilesFromStore(fileStore, opts.Path,
		file.FilenameRegex(d.FilenameExtension()))
	if err != nil {
		d.Close() // TODO what happens with errors from this func?
		return nil, nil, 0, err
	}
	version, err := d.Version()
	if err != nil {
		d.Close() // TODO what happens with errors from this func?
		return nil, nil, 0, err
	}
	return d, &files, version, nil
}

// NewPipe is a convenience function for pipe.New().
// This is helpful if the user just wants to import this package and nothing else.
func NewPipe() chan interface{} {
	return pipep.New()
}

// interrupts is an internal variable that holds the state of
// interrupt handling
var interrupts = true

// Graceful enables interrupts checking. Once the first ^C is received
// it will finish the currently running migration and abort execution
// of the next migration. If ^C is received twice, it will stop
// execution immediately.
func Graceful() {
	interrupts = true
}

// NonGraceful disables interrupts checking. The first received ^C will
// stop execution immediately.
func NonGraceful() {
	interrupts = false
}

// File store to read migration scripts from
var fileStore file.FileStore = &file.FSStore{}

// Read migration scripts from a given file store.
//
// In order to use bindata asset as a store:
// 	import "github.com/PlanitarInc/migrate/migrate"
// 	import "github.com/PlanitarInc/migrate/file"
// 	...
// 	migrate.UseStore(file.AssetStore{
// 		Asset: Asset,
// 		AssetDir: AssetDir,
// 	})
func UseStore(store file.FileStore) {
	fileStore = store
}

// interrupts returns a signal channel if interrupts checking is
// enabled. nil otherwise.
func handleInterrupts() chan os.Signal {
	if interrupts {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		return c
	}
	return nil
}
