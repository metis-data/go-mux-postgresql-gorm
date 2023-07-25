package main

import (
	"context"
	"database/sql"
	"fmt"
	"go-mux-postgresql-gorm/controllers"
	"go-mux-postgresql-gorm/database"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
)

type App struct {
	Router  *mux.Router
	DB      *sql.DB
	Tp      *sdktrace.TracerProvider
	Handler http.Handler
}

func newExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	return otlptracehttp.New(ctx, otlptracehttp.WithInsecure())
}

func newTraceProvider(exp sdktrace.SpanExporter) *sdktrace.TracerProvider {
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("gorm"),
		),
	)

	if err != nil {
		panic(err)
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	)
}

func (a *App) Initialize() {
	godotenv.Load()

	// Initialize Database
	database.Connect(os.Getenv("DATABASE_URL"))
	database.Migrate()

	// Initialize the router
	router := mux.NewRouter().StrictSlash(true)

	// Register Routes
	RegisterProductRoutes(router)

	// Configure OTel trace provider
	var err error

	ctx := context.Background()
	exp, err := newExporter(ctx)
	if err != nil {
		log.Fatalf("failed to initialize exporter: %v", err)
	}

	a.Tp= newTraceProvider(exp)
	if err != nil {
		log.Fatal(err)
	}

	otel.SetTracerProvider(a.Tp)
	router.Use(otelmux.Middleware("go", otelmux.WithTracerProvider(a.Tp)))

	// Create CORS
	var corsSettings = handlers.AllowedOrigins([]string{
		"*",
	})

	// Create final handler
	a.Router = router
	a.Handler = handlers.CORS(corsSettings)(router)
}

func RegisterProductRoutes(router *mux.Router) {
	router.HandleFunc("/titles/ratings/best", controllers.GetBestMovies).Methods("GET")
	router.HandleFunc("/titles/ratingsIndexed/best", controllers.GetBestMoviesIndexed).Methods("GET")
	router.HandleFunc("/titles", controllers.GetTitles).Methods("GET")
	router.HandleFunc("/titlesForAnActor", controllers.TitlesForAnActor).Methods("GET")
	router.HandleFunc("/highestRatedMoviesForAnActor", controllers.HighestRatedMoviesForAnActor).Methods("GET")
	router.HandleFunc("/highestRatedMovies", controllers.HighestRatedMovies).Methods("GET")
	router.HandleFunc("/commonMoviesForTwoActors", controllers.CommonMoviesForTwoActors).Methods("GET")
	router.HandleFunc("/crewOfGivenMovie", controllers.CrewOfGivenMovie).Methods("GET")
	router.HandleFunc("/mostProlificActorInPeriod", controllers.MostProlificActorInPeriod).Methods("GET")
	router.HandleFunc("/mostProlificActorInGenre", controllers.MostProlificActorInGenre).Methods("GET")
	router.HandleFunc("/mostCommonTeammates", controllers.MostCommonTeammates).Methods("GET")
}

func (a *App) Run() {
	log.Println(fmt.Sprintf("Starting Server on port 3000"))
	log.Fatal(http.ListenAndServe("127.0.0.1:3000", a.Handler))
}

func main() {
	a := App{}
	a.Initialize()
	a.Run()
}
