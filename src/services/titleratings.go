package services

import (
	"go-mux-postgresql-gorm/entities"

	"gorm.io/gorm"
)

func GetBestMovies(db *gorm.DB) []entities.TitleRating {
	var titleRatings []entities.TitleRating
	db.Where("averagerating = 10.0").Find(&titleRatings)
	return titleRatings
}

func GetBestMoviesIndexed(db *gorm.DB) []entities.TitleRatingIndexed {
	var titleRatings []entities.TitleRatingIndexed
	db.Where("averagerating = 10.0").Find(&titleRatings)
	return titleRatings
}
