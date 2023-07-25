package entities

type NameBasic struct {
	Nconst            string `json:"nconst"`
	Primaryname       string `json:"primaryname"`
	Birthyear         int32  `json:"birthyear"`
	Deathyear         int32  `json:"deathyear"`
	Primaryprofession string `json:"primaryprofession"`
	Knownfortitles    string `json:"knownfortitles"`
}
