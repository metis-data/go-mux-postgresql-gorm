package database

import (
	"log"
	"os"
	"time"

	"fmt"
	"io/ioutil"
	"path/filepath"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"gorm.io/plugin/opentelemetry/tracing"
)

var Instance *gorm.DB
var err error

func Connect(connectionString string) {
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             time.Nanosecond, // Slow SQL threshold
			LogLevel:                  logger.Info,
			IgnoreRecordNotFoundError: false,
			ParameterizedQueries:      true,
			Colorful:                  false,
		},
	)

	Instance, err = gorm.Open(postgres.Open(connectionString+"?sslmode=disable"), &gorm.Config{
		Logger: newLogger,
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   "imdb.",
			SingularTable: false,
		}})

	if err != nil {
		log.Fatal(err)
		panic("Cannot connect to DB")
	}

	if err := Instance.Use(tracing.NewPlugin()); err != nil {
		panic(err)
	}
}

func Migrate() {
	directoryPath := "database/migrations"

	files, err := ioutil.ReadDir(directoryPath)
	if err != nil {
		fmt.Println("Error:", err)
		panic("Migration error")
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		content, err := ioutil.ReadFile(filepath.Join(directoryPath, file.Name()))
		if err != nil {
			fmt.Printf("Error reading %s: %v\n", file.Name(), err)
			panic("Migration error")
		}

		var sqlQuery = string(content[:])

		fmt.Printf("Running:\n%s\n\n", sqlQuery)
		Instance.Exec(sqlQuery)
	}

	log.Println("Database Migration Completed")
}
