// Package postgres implements the Driver interface.
package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/PlanitarInc/migrate/file"
	"github.com/PlanitarInc/migrate/migrate/direction"
	"github.com/lib/pq"
)

type Driver struct {
	db     *sql.DB
	ownsDB bool
}

const tableName = "schema_migrations"

func (driver *Driver) setDB(instance interface{}, url string) error {
	if instance == nil {
		db, err := sql.Open("postgres", url)
		if err != nil {
			return err
		}

		driver.db = db
		driver.ownsDB = true
		return nil
	}

	db, ok := instance.(*sql.DB)
	if !ok {
		return fmt.Errorf("Expected instance of *sql.DB, got %#v", instance)
	}

	driver.db = db
	return nil
}

func (driver *Driver) Initialize(instance interface{}, url string) error {
	if err := driver.setDB(instance, url); err != nil {
		return err
	}
	if err := driver.db.Ping(); err != nil {
		return err
	}
	if err := driver.ensureVersionTableExists(); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) Close() error {
	if !driver.ownsDB {
		return nil
	}
	if err := driver.db.Close(); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) ensureVersionTableExists() error {
	q := `CREATE TABLE IF NOT EXISTS ` + tableName + ` (
		id text,
		version int not null,
		primary key (id, version)
	)`
	if _, err := driver.db.Exec(q); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) FilenameExtension() string {
	return "sql"
}

func (driver *Driver) Migrate(id string, f file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f

	tx, err := driver.db.Begin()
	if err != nil {
		pipe <- err
		return
	}

	if f.Direction == direction.Up {
		q := `INSERT INTO ` + tableName + ` (id, version) VALUES ($1, $2)`
		if _, err := tx.Exec(q, id, f.Version); err != nil {
			pipe <- err
			if err := tx.Rollback(); err != nil {
				pipe <- err
			}
			return
		}
	} else if f.Direction == direction.Down {
		q := `DELETE FROM ` + tableName + ` WHERE id = $1 AND version = $2`
		if _, err := tx.Exec(q, id, f.Version); err != nil {
			pipe <- err
			if err := tx.Rollback(); err != nil {
				pipe <- err
			}
			return
		}
	}

	if err := f.ReadContent(); err != nil {
		pipe <- err
		return
	}

	if _, err := tx.Exec(string(f.Content)); err != nil {
		pqErr := err.(*pq.Error)
		offset, err := strconv.Atoi(pqErr.Position)
		if err == nil && offset >= 0 {
			lineNo, columnNo := file.LineColumnFromOffset(f.Content, offset-1)
			errorPart := file.LinesBeforeAndAfter(f.Content, lineNo, 5, 5, true)
			pipe <- errors.New(fmt.Sprintf("%s %v: %s in line %v, column %v:\n\n%s", pqErr.Severity, pqErr.Code, pqErr.Message, lineNo, columnNo, string(errorPart)))
		} else {
			pipe <- errors.New(fmt.Sprintf("%s %v: %s", pqErr.Severity, pqErr.Code, pqErr.Message))
		}

		if err := tx.Rollback(); err != nil {
			pipe <- err
		}
		return
	}

	if err := tx.Commit(); err != nil {
		pipe <- err
		return
	}
}

func (driver *Driver) Version(id string) (uint64, error) {
	var version uint64
	err := driver.db.QueryRow(`
		SELECT version FROM `+tableName+`
		WHERE id = $1
		ORDER BY version DESC
		LIMIT 1`, id).Scan(&version)
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		return 0, err
	default:
		return version, nil
	}
}
