package test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
)

import (
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

import (
	"github.com/arana-db/arana/pkg/util/log"
)

const (
	dbUsername string = "root"
	dbPassword string = "123456"
	dbName     string = "employees"
)

var (
	db *sql.DB
)

func SetupMySQLContainer() (func(), *sql.DB, error) {
	log.Info("setup MySQL Container")
	ctx := context.Background()

	seedDataPath, err := os.Getwd()
	if err != nil {
		log.Errorf("error get working directory: %s", err)
		panic(fmt.Sprintf("%v", err))
	}

	mountPath := seedDataPath + "/../docker/scripts"

	req := testcontainers.ContainerRequest{
		Image:        "mysql:latest",
		ExposedPorts: []string{"3306/tcp", "33060/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": dbPassword,
			"MYSQL_DATABASE":      dbName,
		},
		BindMounts: map[string]string{
			"/docker-entrypoint-initdb.d": mountPath,
		},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server - GPL"),
	}

	mysqlC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		log.Errorf("error starting mysql container: %s", err)
		panic(fmt.Sprintf("%v", err))
	}

	closeContainer := func() {
		log.Info("terminating container")
		err := mysqlC.Terminate(ctx)
		if err != nil {
			log.Errorf("error terminating mysql container: %s", err)
			panic(fmt.Sprintf("%v", err))
		}
	}

	host, _ := mysqlC.Host(ctx)
	p, _ := mysqlC.MappedPort(ctx, "3306/tcp")
	port := p.Int()

	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8",
		dbUsername, dbPassword, host, port, dbName)

	db, err = sql.Open("mysql", connectionString)
	if err != nil {
		log.Info("error connect to db: %+v\n", err)
		return closeContainer, db, err
	}

	if err = db.Ping(); err != nil {
		log.Infof("error pinging db: %+v\n", err)
		return closeContainer, db, err
	}

	return closeContainer, db, nil
}
