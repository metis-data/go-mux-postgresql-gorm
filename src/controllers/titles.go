package controllers

import (
	"go-mux-postgresql-gorm/services"
	"net/http"
	"strconv"
)

func GetTitles(w http.ResponseWriter, r *http.Request) {
	ReturnJson(w, services.GetTitles(WrapContext(r), r.URL.Query().Get("title")))
}

func TitlesForAnActor(w http.ResponseWriter, r *http.Request) {
	ReturnJson(w, services.TitlesForAnActor(WrapContext(r), r.URL.Query().Get("nconst"), r.URL.Query().Get("method")))
}

func HighestRatedMoviesForAnActor(w http.ResponseWriter, r *http.Request) {
	ReturnJson(w, services.HighestRatedMoviesForAnActor(WrapContext(r), r.URL.Query().Get("nconst"), r.URL.Query().Get("method")))
}

func HighestRatedMovies(w http.ResponseWriter, r *http.Request) {
	numvotes, _ := strconv.Atoi(r.URL.Query().Get("numvotes"))
	ReturnJson(w, services.HighestRatedMovies(WrapContext(r), numvotes, r.URL.Query().Get("method")))
}

func CommonMoviesForTwoActors(w http.ResponseWriter, r *http.Request) {
	ReturnJson(w, services.CommonMoviesForTwoActors(WrapContext(r), r.URL.Query().Get("actor1"), r.URL.Query().Get("actor2"), r.URL.Query().Get("method")))
}

func CrewOfGivenMovie(w http.ResponseWriter, r *http.Request) {
	ReturnJson(w, services.CrewOfGivenMovie(WrapContext(r), r.URL.Query().Get("tconst"), r.URL.Query().Get("method")))
}

func MostProlificActorInPeriod(w http.ResponseWriter, r *http.Request) {
	startYear, _ := strconv.Atoi(r.URL.Query().Get("startYear"))
	endYear, _ := strconv.Atoi(r.URL.Query().Get("endYear"))
	ReturnJson(w, services.MostProlificActorInPeriod(WrapContext(r), startYear, endYear, r.URL.Query().Get("method")))
}

func MostProlificActorInGenre(w http.ResponseWriter, r *http.Request) {
	ReturnJson(w, services.MostProlificActorInGenre(WrapContext(r), r.URL.Query().Get("genre"), r.URL.Query().Get("method")))
}

func MostCommonTeammates(w http.ResponseWriter, r *http.Request) {
	ReturnJson(w, services.MostCommonTeammates(WrapContext(r), r.URL.Query().Get("nconst"), r.URL.Query().Get("method")))
}
