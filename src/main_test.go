package main

import (
	"fmt"
	"strings"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"
	"github.com/joho/godotenv"
    "github.com/testcontainers/testcontainers-go"
    "github.com/testcontainers/testcontainers-go/network"
	"github.com/docker/docker/api/types/container"
	"context"
)

var a App

func TestMain(m *testing.M) {
	godotenv.Load()

	if os.Getenv("MOCK_CONTAINER_DEPENDENCIES") != "" {
		ctx := context.Background()

		newNetwork, err := network.New(ctx, network.WithCheckDuplicate())
		if err != nil {
			panic(err)
		}
		defer func() {
			newNetwork.Remove(ctx)
		}()

		networkName := newNetwork.Name

		databaseRequest := testcontainers.ContainerRequest{
			Image:        "public.ecr.aws/o2c0x5x8/metis-demo-mini-db:latest",
			ExposedPorts: []string{"5432/tcp", "4318/tcp"},
            Networks: []string{
                networkName,
            },
			Name: "database",
		}
		database, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: databaseRequest,
			Started:          true,
		})
		if err != nil {
			panic(err)
		}

		defer func() {
			if err := database.Terminate(ctx); err != nil {
				panic(err)
			}
		}()

		mappedDatabasePort, err := database.MappedPort(ctx, "5432")
		if err != nil {
			panic(err)
		}

		mappedCollectorPort, err := database.MappedPort(ctx, "4318")
		if err != nil {
			panic(err)
		}

		os.Setenv("DATABASE_URL", strings.Replace(os.Getenv("DATABASE_URL"), "5432", strconv.Itoa(mappedDatabasePort.Int()), 1))

		otelCollectorRequest := testcontainers.ContainerRequest{
			Image:        "public.ecr.aws/o2c0x5x8/metis-otel-collector:latest",
            Networks: []string{
                networkName,
            },
			ConfigModifier: func(config *container.Config) {
				config.Env = []string{
					"METIS_API_KEY=" + os.Getenv("METIS_API_KEY"), 
					"CONNECTION_STRING=postgresql://postgres:postgres@database:5432/demo?schema=imdb",
					"LOG_LEVEL=debug",
				}
			},
		}
		otelCollector, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: otelCollectorRequest,
			Started:          true,
		})
		if err != nil {
			panic(err)
		}

		os.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://127.0.0.1:" + strconv.Itoa(mappedCollectorPort.Int()) + "/")

		defer func() {
			if err := otelCollector.Terminate(ctx); err != nil {
				panic(err)
			}
		}()
	}

	a := App{}
	a.Initialize()
	httptest.NewServer(a.Handler)

	var endpoints []string = []string{
		"/titles/ratings/best",
		"/titles/ratingsIndexed/best",
		"/titles?title=Test",
		"/titlesForAnActor?nconst=nm1588970",
		"/titlesForAnActor?nconst=nm1588970&method=2",
		"/highestRatedMoviesForAnActor?nconst=nm1588970",
		"/highestRatedMoviesForAnActor?nconst=nm1588970&method=2",
		"/highestRatedMovies?numvotes=10000",
		"/highestRatedMovies?numvotes=10000&method=2",
		"/commonMoviesForTwoActors?actor1=nm0302368&actor2=nm0001908",
		"/commonMoviesForTwoActors?actor1=nm0302368&actor2=nm0001908&method=2",
		"/commonMoviesForTwoActors?actor1=nm0302368&actor2=nm0001908&method=3",
		"/crewOfGivenMovie?tconst=tt0000439",
		"/crewOfGivenMovie?tconst=tt0000439&method=2",
		"/crewOfGivenMovie?tconst=tt0000439&method=3",
		"/crewOfGivenMovie?tconst=tt0000439&method=4",
		"/mostProlificActorInPeriod?startYear=1900&endYear=1915",
		"/mostProlificActorInPeriod?startYear=1900&endYear=1915&method=2",
		"/mostProlificActorInPeriod?startYear=1900&endYear=1915&method=3",
		"/mostProlificActorInGenre?genre=Action",
		"/mostProlificActorInGenre?genre=Action&method=2",
		"/mostProlificActorInGenre?genre=Action&method=3",
		"/mostCommonTeammates?nconst=nm0000428",
		"/mostCommonTeammates?nconst=nm0000428&method=2",
	}

	var exitCode = 0

	for _, endpoint := range endpoints {
		fmt.Printf("Testing %s: ", endpoint)

		request, _ := http.NewRequest("GET", endpoint, nil)
		response := httptest.NewRecorder()
		a.Router.ServeHTTP(response, request)

		if response.Code != http.StatusOK {
			fmt.Printf("Received invalid response code %d\n", response.Code)
			exitCode = 1
		} else {
			fmt.Printf("Okay\n")
		}
	}

	time.Sleep(30 * time.Second) 
	os.Exit(exitCode)
}
