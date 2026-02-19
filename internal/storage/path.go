package storage

import (
	"fmt"
	"path"
	"regexp"
	"time"
)

var pathComponentPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

func BuildDataFilePath(tenantID, tableName string, eventTime time.Time, snapshotID int64, sequence int) (string, error) {
	if err := validatePathComponent(tenantID, "tenant id"); err != nil {
		return "", err
	}
	if err := validatePathComponent(tableName, "table name"); err != nil {
		return "", err
	}
	if sequence < 0 {
		return "", fmt.Errorf("sequence must be >= 0")
	}

	ts := eventTime.UTC()
	return path.Join(
		tenantID,
		tableName,
		fmt.Sprintf("date=%04d-%02d-%02d", ts.Year(), ts.Month(), ts.Day()),
		fmt.Sprintf("hour=%02d", ts.Hour()),
		fmt.Sprintf("part-%d-%05d.parquet", snapshotID, sequence),
	), nil
}

func BuildDeleteFilePath(tenantID, tableName string, snapshotID int64, sequence int) (string, error) {
	if err := validatePathComponent(tenantID, "tenant id"); err != nil {
		return "", err
	}
	if err := validatePathComponent(tableName, "table name"); err != nil {
		return "", err
	}
	if sequence < 0 {
		return "", fmt.Errorf("sequence must be >= 0")
	}
	return path.Join(
		tenantID,
		tableName,
		"deletes",
		fmt.Sprintf("delete-%d-%05d.parquet", snapshotID, sequence),
	), nil
}

func validatePathComponent(value, field string) error {
	if !pathComponentPattern.MatchString(value) {
		return fmt.Errorf("invalid %s: %q", field, value)
	}
	return nil
}
