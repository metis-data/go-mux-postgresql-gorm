package entities

type TitleCrew struct {
	Tconst    string `json:"tconst"`
	Directors string `json:"directors"`
	Writers   string `json:"writers"`
}

func (TitleCrew) TableName() string {
	return "imdb.title_crew"
}
