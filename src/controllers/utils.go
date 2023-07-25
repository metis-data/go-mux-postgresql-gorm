package controllers

import (
	"encoding/json"
	"go-mux-postgresql-gorm/database"
	"net/http"

	"gorm.io/gorm"
)

func ReturnJson(w http.ResponseWriter, result any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(result)
}

func WrapContext(r *http.Request) *gorm.DB {
	return database.Instance.WithContext(r.Context())
}
