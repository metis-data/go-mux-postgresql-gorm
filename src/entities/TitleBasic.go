package entities

type TitleBasic struct {
	Tconst         string `json:"tconst"`
	Titletype      string `json:"titletype"`
	Primarytitle   string `json:"primarytitle"`
	Originaltitle  string `json:"originaltitle"`
	Isadult        bool   `json:"isadult"`
	Startyear      int32  `json:"startyear"`
	Endyear        int32  `json:"endyear"`
	Runtimeminutes int32  `json:"runtimeminutes"`
	Genres         string `json:"genres"`
}
