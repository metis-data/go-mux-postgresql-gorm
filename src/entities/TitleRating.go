package entities

type TitleRating struct {
	Tconst          string     `json:"tconst"`
	Averagerating   float64    `json:"averagerating"`
	Numvotes        int32      `json:"numvotes"`
}
