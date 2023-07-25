package controllers

import (
	"go-mux-postgresql-gorm/services"
	"net/http"
)

func GetBestMovies(w http.ResponseWriter, r *http.Request) {
	ReturnJson(w, services.GetBestMovies(WrapContext(r)))
}

func GetBestMoviesIndexed(w http.ResponseWriter, r *http.Request) {
	ReturnJson(w, services.GetBestMoviesIndexed(WrapContext(r)))
}
