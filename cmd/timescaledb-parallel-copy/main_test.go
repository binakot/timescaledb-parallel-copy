package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/ory/dockertest/v3"
)

var (
	testDb     *sql.DB
	testDbPort string
)

func TestMain(m *testing.M) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.Run("postgres", "13", []string{"POSTGRES_PASSWORD=postgres"})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		testDbPort = resource.GetPort("5432/tcp")
		testDb, err = sql.Open("postgres", fmt.Sprintf("postgres://postgres:postgres@localhost:%s/postgres?sslmode=disable", testDbPort))
		if err != nil {
			return err
		}
		return testDb.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

// TODO https://github.com/timescale/timescaledb-parallel-copy/issues/50
func TestSingleMultilineRecordSingleWorkerBatchSizeOne(t *testing.T) {
	err := IntTest("../test-dataset/multiline_single_record.csv",
		"CREATE TABLE test (id SERIAL PRIMARY KEY, state TEXT, time TIMESTAMPTZ, date TEXT)",
		1,
		1,
		1,
		testDbPort)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestSingleMultilineRecordSingleWorkerBatchSizeOneThousand(t *testing.T) {
	err := IntTest("../test-dataset/multiline_single_record.csv",
		"CREATE TABLE test (id SERIAL PRIMARY KEY, state TEXT, time TIMESTAMPTZ, date TEXT)",
		1,
		1,
		1000,
		testDbPort)
	if err != nil {
		t.Errorf(err.Error())
	}
}

// TODO https://github.com/timescale/timescaledb-parallel-copy/issues/50
func TestSingleMultilineRecordTwoWorkersBatchSizeOne(t *testing.T) {
	err := IntTest("../test-dataset/multiline_single_record.csv",
		"CREATE TABLE test (id SERIAL PRIMARY KEY, state TEXT, time TIMESTAMPTZ, date TEXT)",
		1,
		2,
		1,
		testDbPort)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestSingleMultilineRecordTwoWorkersBatchSizeOneThousand(t *testing.T) {
	err := IntTest("../test-dataset/multiline_single_record.csv",
		"CREATE TABLE test (id SERIAL PRIMARY KEY, state TEXT, time TIMESTAMPTZ, date TEXT)",
		1,
		2,
		1000,
		testDbPort)
	if err != nil {
		t.Errorf(err.Error())
	}
}

// TODO https://github.com/timescale/timescaledb-parallel-copy/issues/50
func TestFiveMultilineRecordsSingleWorkerBatchSizeOne(t *testing.T) {
	err := IntTest("../test-dataset/multiline_five_records.csv",
		"CREATE TABLE test (id SERIAL PRIMARY KEY, state TEXT, time TIMESTAMPTZ, date TEXT)",
		5,
		1,
		1,
		testDbPort)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestFiveMultilineRecordsSingleWorkerBatchSizeOneThousand(t *testing.T) {
	err := IntTest("../test-dataset/multiline_five_records.csv",
		"CREATE TABLE test (id SERIAL PRIMARY KEY, state TEXT, time TIMESTAMPTZ, date TEXT)",
		5,
		1,
		1000,
		testDbPort)
	if err != nil {
		t.Errorf(err.Error())
	}
}

// TODO https://github.com/timescale/timescaledb-parallel-copy/issues/50
func TestFiveMultilineRecordsTwoWorkersBatchSizeOne(t *testing.T) {
	err := IntTest("../test-dataset/multiline_five_records.csv",
		"CREATE TABLE test (id SERIAL PRIMARY KEY, state TEXT, time TIMESTAMPTZ, date TEXT)",
		5,
		2,
		1,
		testDbPort)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestFiveMultilineRecordsTwoWorkersBatchSizeOneThousand(t *testing.T) {
	err := IntTest("../test-dataset/multiline_five_records.csv",
		"CREATE TABLE test (id SERIAL PRIMARY KEY, state TEXT, time TIMESTAMPTZ, date TEXT)",
		5,
		2,
		1000,
		testDbPort)
	if err != nil {
		t.Errorf(err.Error())
	}
}

// TODO https://github.com/timescale/timescaledb-parallel-copy/issues/24
func TestRecordsWithJSON(t *testing.T) {
	err := IntTest("../test-dataset/json_records.csv",
		"CREATE TABLE test (id UUID, dataset_id UUID, time TIMESTAMPTZ, nsec BIGINT, data JSONB, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ)",
		4,
		1,
		1,
		testDbPort)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func IntTest(csvFileName string, tableCreateQuery string, expectedRowCount int, workerCount int, batchSize int, dbPort string) error {
	_, err := testDb.Exec("DROP TABLE IF EXISTS test")
	if err != nil {
		return err
	}
	_, err = testDb.Exec(tableCreateQuery)
	if err != nil {
		return err
	}

	os.Args = []string{"timescaledb-parallel-copy",
		"--connection", fmt.Sprintf("host=localhost port=%s user=postgres password=postgres sslmode=disable", dbPort),
		"--db-name", "postgres",
		"--table", "test",
		"--file", csvFileName,
		"--workers", strconv.Itoa(workerCount),
		"--batch-size", strconv.Itoa(batchSize),
	}
	main()

	var actualRowCount int
	err = testDb.QueryRow("SELECT count(*) FROM test").Scan(&actualRowCount)
	if err != nil {
		return err
	}

	if actualRowCount != expectedRowCount {
		return err
	}

	return nil
}

func ReadTestFile(fileName string) ([]string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	err = file.Close()
	if err != nil {
		return nil, err
	}

	return lines, err
}
