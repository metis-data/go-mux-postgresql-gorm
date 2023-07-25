package entities

type TitlePrincipal struct {
	Tconst     string `json:"tconst"`
	Ordering   int32  `json:"ordering"`
	Nconst     string `json:"nconst"`
	Category   string `json:"category"`
	Job        string `json:"job"`
	Characters string `json:"characters"`
}
