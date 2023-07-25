package entities

type TitleRatingIndexed struct {
	Tconst        string  `json:"tconst"`
	Averagerating float64 `json:"averagerating"`
	Numvotes      int32   `json:"numvotes"`
}

func (TitleRatingIndexed) TableName() string {
	return "imdb.title_ratings_indexed"
}
