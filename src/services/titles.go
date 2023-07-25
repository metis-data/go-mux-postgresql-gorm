package services

import (
	"database/sql"
	"go-mux-postgresql-gorm/entities"
	"sort"
	"strings"

	"github.com/samber/lo"
	"gorm.io/gorm"
)

func GetTitles(db *gorm.DB, name string) []entities.TitleBasic {
	var result []entities.TitleBasic
	db.Where("primarytitle LIKE ?", "%"+name+"%").Find(&result)
	return result
}

func TitlesForAnActor(db *gorm.DB, nconst string, method string) []entities.TitleBasic {
	var titlesForAnActorNaive = func() []entities.TitleBasic {
		var result []entities.TitleBasic
		db.
			Model(&entities.TitleBasic{}).
			Joins("LEFT JOIN imdb.title_principals ON title_principals.tconst = title_basics.tconst").
			Where("nconst = ?", nconst).
			Order("startyear DESC").
			Limit(10).
			Find(&result)
		return result
	}

	var titlesForAnActorManual = func() []entities.TitleBasic {
		db.Exec("CREATE INDEX IF NOT EXISTS title_principals_nconst_idx ON imdb.title_principals(nconst) INCLUDE (tconst)")

		var result []entities.TitleBasic
		db.Raw("SELECT TitleBasic.*\n"+
			"FROM imdb.title_basics AS TitleBasic\n"+
			"JOIN imdb.title_principals AS TitlePrincipals ON TitlePrincipals.tconst = TitleBasic.tconst\n"+
			"WHERE TitlePrincipals.nconst = ?\n"+
			"ORDER BY TitleBasic.startyear DESC\n"+
			"LIMIT 10", nconst).Scan(&result)

		return result
	}

	if method == "" { 
		return titlesForAnActorNaive()
	} else {
		return titlesForAnActorManual()
	}
}

func HighestRatedMoviesForAnActor(db *gorm.DB, nconst string, method string) []entities.TitleBasic {
	var highestRatedMoviesForAnActorNaive = func() []entities.TitleBasic {
		var result []entities.TitleBasic
		db.
			Model(&entities.TitleBasic{}).
			Joins("LEFT JOIN imdb.title_ratings ON title_ratings.tconst = title_basics.tconst").
			Joins("LEFT JOIN imdb.title_principals ON title_principals.tconst = title_basics.tconst").
			Where("nconst = ?", nconst).
			Order("averagerating DESC").
			Limit(10).
			Find(&result)
		return result
	}

	var highestRatedMoviesForAnActorWithIndex = func() []entities.TitleBasic {
		db.Exec("CREATE INDEX IF NOT EXISTS title_principals_nconst_idx ON imdb.title_principals(nconst) INCLUDE (tconst)")

		var result []entities.TitleBasic
		db.
			Model(&entities.TitleBasic{}).
			Joins("LEFT JOIN imdb.title_ratings ON title_ratings.tconst = title_basics.tconst").
			Joins("LEFT JOIN imdb.title_principals ON title_principals.tconst = title_basics.tconst").
			Where("nconst = ?", nconst).
			Order("averagerating DESC").
			Limit(10).
			Find(&result)

		return result
	}

	if method == "" { 
		return highestRatedMoviesForAnActorNaive()
	} else {
		return highestRatedMoviesForAnActorWithIndex()
	}
}

func HighestRatedMovies(db *gorm.DB, numVotes int, method string) []entities.TitleBasic {
	var highestRatedMoviesNaive = func() []entities.TitleBasic {
		var result []entities.TitleBasic
		db.
			Model(&entities.TitleBasic{}).
			Joins("LEFT JOIN imdb.title_ratings ON title_ratings.tconst = title_basics.tconst").
			Where("numvotes > ?", numVotes).
			Order("averagerating DESC").
			Find(&result)
		return result
	}

	var highestRatedMoviesWithIndex = func() []entities.TitleBasic {
		db.Exec("CREATE INDEX IF NOT EXISTS IDX_title_ratings ON imdb.title_ratings (numvotes)")

		var result []entities.TitleBasic
		db.
			Model(&entities.TitleBasic{}).
			Joins("LEFT JOIN imdb.title_ratings ON title_ratings.tconst = title_basics.tconst").
			Where("numvotes > ?", numVotes).
			Order("averagerating DESC").
			Find(&result)
		return result
	}

	if method == "" { 
		return highestRatedMoviesNaive()
	} else {
		return highestRatedMoviesWithIndex()
	}
}

func CommonMoviesForTwoActors(db *gorm.DB, actor1 string, actor2 string, method string) []entities.TitleBasic {
	var commonMoviesForTwoActorsInApp = func() []entities.TitleBasic {
		var first []entities.TitlePrincipal
		db.
			Model(&entities.TitlePrincipal{}).
			Where("nconst = ?", actor1).
			Find(&first)

		var second []entities.TitlePrincipal
		db.
			Model(&entities.TitlePrincipal{}).
			Where("nconst = ?", actor2).
			Find(&second)

		var firstTconsts []string

		for _, item := range first {
			firstTconsts = append(firstTconsts, item.Tconst)
		}

		var secondTconsts []string

		for _, item := range second {
			secondTconsts = append(secondTconsts, item.Tconst)
		}

		var result []entities.TitleBasic
		db.
			Model(&entities.TitleBasic{}).
			Where("tconst IN ?", firstTconsts).
			Where("tconst IN ?", secondTconsts).
			Find(&result)

		return result
	}

	var commonMoviesForTwoActorsInAppOptimized = func() []entities.TitleBasic {
		var first []entities.TitlePrincipal
		db.
			Model(&entities.TitlePrincipal{}).
			Where("nconst = ?", actor1).
			Find(&first)

		var second []entities.TitlePrincipal
		db.
			Model(&entities.TitlePrincipal{}).
			Where("nconst = ?", actor2).
			Find(&second)

		knownTitles := map[string]struct{}{}

		for _, item := range first {
			knownTitles[item.Tconst] = struct{}{}
		}

		var finalTconsts []string

		for _, item := range second {
			if _, exists := knownTitles[item.Tconst]; exists {
				finalTconsts = append(finalTconsts, item.Tconst)
			}
		}

		var result []entities.TitleBasic
		db.
			Model(&entities.TitleBasic{}).
			Where("tconst IN ?", finalTconsts).
			Find(&result)

		return result
	}

	var commonMoviesForTwoActorsManual = func() []entities.TitleBasic {
		var result []entities.TitleBasic
		db.
			Model(&entities.TitleBasic{}).
			Joins("JOIN imdb.title_principals AS TP1 ON TP1.tconst = title_basics.tconst").
			Joins("JOIN imdb.title_principals AS TP2 ON TP2.tconst = title_basics.tconst").
			Where("TP1.nconst = ?", actor1).
			Where("TP2.nconst = ?", actor2).
			Find(&result)
		return result
	}

	if method == "" { 
		return commonMoviesForTwoActorsInApp()
	} else if method == "2" {
		return commonMoviesForTwoActorsInAppOptimized()
	} else {
		return commonMoviesForTwoActorsManual()
	}
}

func CrewOfGivenMovie(db *gorm.DB, tconst string, method string) []entities.NameBasic {
	var crewOfGivenMovieManualSlow = func() []entities.NameBasic {
		var result []entities.NameBasic

		db.Raw("SELECT DISTINCT NB.*\n"+
			"FROM imdb.title_basics AS TB\n"+
			"LEFT JOIN imdb.title_principals AS TP ON TP.tconst = TB.tconst\n"+
			"LEFT JOIN imdb.title_crew AS TC ON TC.tconst = TB.tconst\n"+
			"LEFT JOIN imdb.name_basics AS NB ON \n"+
			"		NB.nconst = TP.nconst \n"+
			"		OR TC.directors = NB.nconst\n"+
			"		OR TC.directors LIKE NB.nconst || ',%'::text\n"+
			"		OR TC.directors LIKE '%,'::text || NB.nconst || ',%'::text\n"+
			"		OR TC.directors LIKE '%,'::text || NB.nconst\n"+
			"		OR TC.writers = NB.nconst\n"+
			"		OR TC.writers LIKE NB.nconst || ',%'::text\n"+
			"		OR TC.writers LIKE '%,'::text || NB.nconst || ',%'::text\n"+
			"		OR TC.writers LIKE '%,'::text || NB.nconst\n"+
			"WHERE TB.tconst = ?", tconst).
			Scan(&result)

		return result
	}

	var crewOfGivenMovieWithUnions = func() []entities.NameBasic {
		var result []entities.NameBasic

		db.Raw("SELECT DISTINCT NB.*\n"+
			"	FROM imdb.title_principals AS TP\n"+
			"	JOIN (\n"+
			"	SELECT tconst, directors, writers\n"+
			"	FROM imdb.title_crew\n"+
			"	WHERE tconst = @tconst\n"+
			"	) AS TC ON TC.tconst = TP.tconst\n"+
			"	LEFT JOIN imdb.name_basics AS NB ON NB.nconst = TP.nconst\n"+
			"UNION\n"+
			"	SELECT DISTINCT NB.*\n"+
			"	FROM imdb.title_principals AS TP\n"+
			"	JOIN (\n"+
			"	SELECT tconst, directors, writers\n"+
			"	FROM imdb.title_crew\n"+
			"	WHERE tconst = @tconst\n"+
			"	) AS TC ON TC.tconst = TP.tconst\n"+
			"	LEFT JOIN imdb.name_basics AS NB ON TC.directors LIKE NB.nconst || ',%'::text\n"+
			"UNION\n"+
			"	SELECT DISTINCT NB.*\n"+
			"	FROM imdb.title_principals AS TP\n"+
			"	JOIN (\n"+
			"	SELECT tconst, directors, writers\n"+
			"	FROM imdb.title_crew\n"+
			"	WHERE tconst = @tconst\n"+
			"	) AS TC ON TC.tconst = TP.tconst\n"+
			"	LEFT JOIN imdb.name_basics AS NB ON TC.directors LIKE '%,'::text || NB.nconst || ',%'::text\n"+
			"UNION\n"+
			"	SELECT DISTINCT NB.*\n"+
			"	FROM imdb.title_principals AS TP\n"+
			"	JOIN (\n"+
			"	SELECT tconst, directors, writers\n"+
			"	FROM imdb.title_crew\n"+
			"	WHERE tconst = @tconst\n"+
			"	) AS TC ON TC.tconst = TP.tconst\n"+
			"	LEFT JOIN imdb.name_basics AS NB ON TC.directors LIKE '%,'::text || NB.nconst\n"+
			"UNION\n"+
			"	SELECT DISTINCT NB.*\n"+
			"	FROM imdb.title_principals AS TP\n"+
			"	JOIN (\n"+
			"	SELECT tconst, directors, writers\n"+
			"	FROM imdb.title_crew\n"+
			"	WHERE tconst = @tconst\n"+
			"	) AS TC ON TC.tconst = TP.tconst\n"+
			"	LEFT JOIN imdb.name_basics AS NB ON TC.writers = NB.nconst\n"+
			"UNION\n"+
			"	SELECT DISTINCT NB.*\n"+
			"	FROM imdb.title_principals AS TP\n"+
			"	JOIN (\n"+
			"	SELECT tconst, directors, writers\n"+
			"	FROM imdb.title_crew\n"+
			"	WHERE tconst = @tconst\n"+
			"	) AS TC ON TC.tconst = TP.tconst\n"+
			"	LEFT JOIN imdb.name_basics AS NB ON TC.writers LIKE NB.nconst || ',%'::text\n"+
			"UNION\n"+
			"	SELECT DISTINCT NB.*\n"+
			"	FROM imdb.title_principals AS TP\n"+
			"	JOIN (\n"+
			"	SELECT tconst, directors, writers\n"+
			"	FROM imdb.title_crew\n"+
			"	WHERE tconst = @tconst\n"+
			"	) AS TC ON TC.tconst = TP.tconst\n"+
			"	LEFT JOIN imdb.name_basics AS NB ON TC.writers LIKE '%,'::text || NB.nconst || ',%'::text\n"+
			"UNION\n"+
			"	SELECT DISTINCT NB.*\n"+
			"	FROM imdb.title_principals AS TP\n"+
			"	JOIN (\n"+
			"	SELECT tconst, directors, writers\n"+
			"	FROM imdb.title_crew\n"+
			"	WHERE tconst = @tconst\n"+
			"	) AS TC ON TC.tconst = TP.tconst\n"+
			"	LEFT JOIN imdb.name_basics AS NB ON TC.writers LIKE '%,'::text || NB.nconst", sql.Named("tconst", tconst)).
			Scan(&result)

		return result
	}

	var crewOfGivenMovieInAppCode = func() []entities.NameBasic {
		knownNames := map[string]struct{}{}

		var crewViaTitlePrincipalsEntities []entities.TitlePrincipal
		db.
			Model(&entities.TitlePrincipal{}).
			Where("tconst = ?", tconst).
			Find(&crewViaTitlePrincipalsEntities)

		var allMatchingNames []string

		for _, item := range crewViaTitlePrincipalsEntities {
			if _, exists := knownNames[item.Nconst]; !exists {
				knownNames[item.Nconst] = struct{}{}
				allMatchingNames = append(allMatchingNames, item.Nconst)
			}
		}

		var crewViaTitleCrewEntities []entities.TitleCrew
		db.
			Model(&entities.TitleCrew{}).
			Where("tconst = ?", tconst).
			Find(&crewViaTitleCrewEntities)

		for _, item := range crewViaTitleCrewEntities {
			for _, director := range strings.Split(item.Directors, ",") {
				if _, exists := knownNames[director]; !exists {
					knownNames[director] = struct{}{}
					allMatchingNames = append(allMatchingNames, director)
				}
			}

			for _, writer := range strings.Split(item.Writers, ",") {
				if _, exists := knownNames[writer]; !exists {
					allMatchingNames = append(allMatchingNames, writer)
				}
			}
		}

		var result []entities.NameBasic
		db.
			Model(&entities.NameBasic{}).
			Where("nconst IN ?", allMatchingNames).
			Find(&result)

		return result
	}

	var crewOfGivenMovieManualFast = func() []entities.NameBasic {
		var result []entities.NameBasic

		db.Raw("WITH RECURSIVE numbers AS (\n"+
			"	SELECT 1 AS number\n"+
			"	UNION ALL\n"+
			"	SELECT number + 1 AS number FROM numbers WHERE number < 1500\n"+
			"),\n"+
			"split_associations AS (\n"+
			"	  SELECT SPLIT_PART(TC.directors, ',', N.number) AS nconst\n"+
			"	  FROM imdb.title_crew AS TC\n"+
			"	  CROSS JOIN numbers AS N\n"+
			"	  WHERE tconst = @tconst AND directors IS NOT NULL AND CHAR_LENGTH(directors) - CHAR_LENGTH(REPLACE(directors, ',', '')) + 1 >= N.number\n"+
			"	UNION\n"+
			"	  SELECT SPLIT_PART(TC.writers, ',', N.number) AS nconst\n"+
			"	  FROM imdb.title_crew AS TC\n"+
			"	  CROSS JOIN numbers AS N\n"+
			"	  WHERE tconst = @tconst AND writers IS NOT NULL AND CHAR_LENGTH(writers) - CHAR_LENGTH(REPLACE(writers, ',', '')) + 1 >= N.number\n"+
			"),\n"+
			"all_associations AS (\n"+
			"	SELECT SA.nconst\n"+
			"	FROM split_associations AS SA\n"+
			"	UNION\n"+
			"	SELECT TP.nconst\n"+
			"	FROM imdb.title_principals AS TP\n"+
			"	WHERE TP.tconst = @tconst\n"+
			")\n"+
			"SELECT NB.*\n"+
			"FROM imdb.name_basics AS NB\n"+
			"JOIN all_associations AS AA ON AA.nconst = NB.nconst", sql.Named("tconst", tconst)).
			Scan(&result)

		return result
	}

	if method == "" { 
		return crewOfGivenMovieManualSlow()
	} else if method == "2" {
		return crewOfGivenMovieWithUnions()
	} else if method == "3" {
		return crewOfGivenMovieInAppCode()
	} else {
		return crewOfGivenMovieManualFast()
	}
}

func MostProlificActorInPeriod(db *gorm.DB, startYear int, endYear int, method string) []entities.NameBasic {
	var mostProlificActorInPeriodInApp = func() []entities.NameBasic {
		var titlesMatchingPeriodEntities []entities.TitleBasic
		db.
			Model(&entities.TitleBasic{}).
			Where("startyear >= ?", startYear).
			Where("startyear <= ?", endYear).
			Find(&titlesMatchingPeriodEntities)

		var titlesMatchingPeriod = lo.Map(titlesMatchingPeriodEntities, func(x entities.TitleBasic, index int) string {
			return x.Tconst
		})

		var principals []entities.TitlePrincipal

		for _, chunk := range lo.Chunk(titlesMatchingPeriod, 10000) {
			var chunkResult []entities.TitlePrincipal
			db.
				Model(&entities.TitlePrincipal{}).
				Where("tconst IN ?", chunk).
				Find(&chunkResult)

			principals = append(principals, chunkResult...)
		}

		counts := make(map[string]int)
		for _, principal := range principals {
			counts[principal.Nconst] = counts[principal.Nconst] + 1
		}

		var countsWithKeys []lo.Tuple2[string, int]
		for _, key := range lo.Keys(counts) {
			countsWithKeys = append(countsWithKeys, lo.T2(key, counts[key]))
		}

		sort.SliceStable(countsWithKeys, func(i, j int) bool {
			return countsWithKeys[i].B > countsWithKeys[j].B
		})

		topResults := lo.Subset(countsWithKeys, 0, 1)

		var result []entities.NameBasic
		db.
			Model(&entities.NameBasic{}).
			Where("nconst IN ?", lo.Map(topResults, func(x lo.Tuple2[string, int], index int) string {
				return x.A
			})).
			Find(&result)

		return result
	}

	var mostProlificActorInPeriodManual = func() []entities.NameBasic {
		var result []entities.NameBasic

		db.Raw("SELECT NB.nconst, MAX(NB.primaryname) AS primaryname, MAX(nb.birthyear) AS birthyear, MAX(NB.deathyear) AS deathyear, MAX(nb.primaryprofession) AS primaryprofession, COUNT(*) AS number_of_titles\n"+
			"FROM imdb.title_basics AS TB\n"+
			"RIGHT JOIN imdb.title_principals AS TP ON TP.tconst = TB.tconst\n"+
			"RIGHT JOIN imdb.name_basics AS NB ON NB.nconst = TP.nconst\n"+
			"WHERE TB.startyear >= @startyear AND TB.startyear <= @endyear\n"+
			"GROUP BY NB.nconst\n"+
			"ORDER BY number_of_titles DESC\n"+
			"LIMIT 1", sql.Named("startyear", startYear), sql.Named("endyear", endYear)).
			Scan(&result)

		return result
	}

	var mostProlificActorInPeriodManualOptimized = func() []entities.NameBasic {
		var result []entities.NameBasic

		db.Raw("WITH best_actor AS (\n"+
			"		SELECT TP.nconst, COUNT(*) AS number_of_titles\n"+
			"		FROM imdb.title_basics AS TB\n"+
			"		LEFT JOIN imdb.title_principals AS TP ON TP.tconst = TB.tconst\n"+
			"		WHERE TB.startyear >= @startyear AND TB.startyear <= @endyear AND TP.nconst IS NOT NULL\n"+
			"		GROUP BY TP.nconst\n"+
			"		ORDER BY number_of_titles DESC\n"+
			"		LIMIT 1\n"+
			")\n"+
			"SELECT BA.nconst, BA.number_of_titles, NB.primaryname, nb.birthyear, NB.deathyear, nb.primaryprofession\n"+
			"FROM best_actor AS BA\n"+
			"JOIN imdb.name_basics AS NB ON NB.nconst = BA.nconst", sql.Named("startyear", startYear), sql.Named("endyear", endYear)).
			Scan(&result)

		return result
	}

	if method == "" { 
		return mostProlificActorInPeriodInApp()
	} else if method == "2" {
		return mostProlificActorInPeriodManual()
	} else {
		return mostProlificActorInPeriodManualOptimized()
	}
}

func MostProlificActorInGenre(db *gorm.DB, genre string, method string) []entities.NameBasic {
	var mostProlificActorInGenreInApp = func() []entities.NameBasic {
		var titlesMatchingGenreEntities []entities.TitleBasic
		db.
			Model(&entities.TitleBasic{}).
			Where("genres LIKE ?", "%"+genre+"%").
			Find(&titlesMatchingGenreEntities)

		var titlesMatchingGenre = lo.Map(lo.Filter(titlesMatchingGenreEntities, func(x entities.TitleBasic, index int) bool {
			return lo.Contains(strings.Split(x.Genres, ","), genre)
		}), func(x entities.TitleBasic, index int) string {
			return x.Tconst
		})

		var principals []entities.TitlePrincipal

		for _, chunk := range lo.Chunk(titlesMatchingGenre, 10000) {
			var chunkResult []entities.TitlePrincipal
			db.
				Model(&entities.TitlePrincipal{}).
				Where("tconst IN ?", chunk).
				Find(&chunkResult)

			principals = append(principals, chunkResult...)
		}

		counts := make(map[string]int)
		for _, principal := range principals {
			counts[principal.Nconst] = counts[principal.Nconst] + 1
		}

		var countsWithKeys []lo.Tuple2[string, int]
		for _, key := range lo.Keys(counts) {
			countsWithKeys = append(countsWithKeys, lo.T2(key, counts[key]))
		}

		sort.SliceStable(countsWithKeys, func(i, j int) bool {
			return countsWithKeys[i].B > countsWithKeys[j].B
		})

		topResults := lo.Subset(countsWithKeys, 0, 10)

		var result []entities.NameBasic
		db.
			Model(&entities.NameBasic{}).
			Where("nconst IN ?", lo.Map(topResults, func(x lo.Tuple2[string, int], index int) string {
				return x.A
			})).
			Find(&result)

		return result
	}

	var mostProlificActorInGenreManual = func() []entities.NameBasic {
		var result []entities.NameBasic

		db.Raw("SELECT NB.nconst, NB.primaryname, NB.birthyear, COUNT(*) AS movies_count\n"+
			"FROM imdb.name_basics AS NB\n"+
			"LEFT JOIN imdb.title_principals AS TP ON TP.nconst = NB.nconst\n"+
			"LEFT JOIN imdb.title_basics AS TB ON TB.tconst = TP.tconst\n"+
			"WHERE TB.genres = @genre OR TB.genres LIKE (@genre || ',%') OR TB.genres LIKE ('%,' || @genre || ',%') OR TB.genres LIKE ('%,' || @genre)\n"+
			"GROUP BY NB.nconst, NB.primaryname, NB.birthyear\n"+
			"ORDER BY movies_count DESC\n"+
			"LIMIT 10", sql.Named("genre", genre)).
			Scan(&result)

		return result
	}

	var mostProlificActorInGenreManualOptimized = func() []entities.NameBasic {
		var result []entities.NameBasic

		db.Raw("WITH best_actors AS (\n"+
			"	SELECT TP.nconst, COUNT(*) AS movies_count\n"+
			"	FROM imdb.title_basics AS TB\n"+
			"	LEFT JOIN imdb.title_principals AS TP ON TP.tconst = TB.tconst\n"+
			"	WHERE TB.genres = @genre OR TB.genres LIKE (@genre || ',%') OR TB.genres LIKE ('%,' || @genre || ',%') OR TB.genres LIKE ('%,' || @genre)\n"+
			"	GROUP BY TP.nconst\n"+
			"	ORDER BY movies_count DESC\n"+
			"	LIMIT 10\n"+
			"  )\n"+
			"  SELECT BA.nconst, NB.primaryname, NB.birthyear, BA.movies_count\n"+
			"  FROM best_actors AS BA\n"+
			"  JOIN imdb.name_basics AS NB ON NB.nconst = BA.nconst\n"+
			"  ORDER BY movies_count DESC", sql.Named("genre", genre)).
			Scan(&result)

		return result
	}

	if method == "" { 
		return mostProlificActorInGenreInApp()
	} else if method == "2" {
		return mostProlificActorInGenreManual()
	} else {
		return mostProlificActorInGenreManualOptimized()
	}
}

func MostCommonTeammates(db *gorm.DB, name string, method string) []entities.NameBasic {
	var mostCommonTeammatesManual = func() []entities.NameBasic {
		var result []entities.NameBasic

		db.Raw("WITH RECURSIVE numbers AS (\n"+
			"	SELECT 1 AS number\n"+
			"	UNION ALL\n"+
			"	SELECT number + 1 AS number FROM numbers WHERE number < 1500\n"+
			"),\n"+
			"titles_for_person AS (\n"+
			"	  SELECT TC.tconst\n"+
			"	  FROM imdb.title_crew AS TC\n"+
			"	  WHERE directors = @nconst OR directors LIKE @nconst || ',%' OR directors LIKE '%,' || @nconst || ',%' OR directors LIKE '%,' || @nconst\n"+
			"	UNION\n"+
			"	  SELECT TC.tconst\n"+
			"	  FROM imdb.title_crew AS TC\n"+
			"	  WHERE writers = @nconst OR writers LIKE @nconst || ',%' OR writers LIKE '%,' || @nconst || ',%' OR writers LIKE '%,' || @nconst\n"+
			"	UNION\n"+
			"	  SELECT tconst\n"+
			"	  FROM imdb.title_principals\n"+
			"	  WHERE nconst = @nconst\n"+
			"),\n"+
			"titles_corresponding AS (\n"+
			"	SELECT TC.tconst, TC.directors, TC.writers\n"+
			"	FROM imdb.title_crew AS TC\n"+
			"	JOIN titles_for_person AS TFP ON TFP.tconst = TC.tconst\n"+
			"),\n"+
			"split_associations AS (\n"+
			"	  SELECT TC.tconst, SPLIT_PART(TC.directors, ',', N.number) AS nconst\n"+
			"	  FROM titles_corresponding AS TC\n"+
			"	  CROSS JOIN numbers AS N\n"+
			"	  WHERE directors IS NOT NULL AND CHAR_LENGTH(directors) - CHAR_LENGTH(REPLACE(directors, ',', '')) + 1 >= N.number\n"+
			"	UNION\n"+
			"	  SELECT TC.tconst, SPLIT_PART(TC.writers, ',', N.number) AS nconst\n"+
			"	  FROM titles_corresponding AS TC\n"+
			"	  CROSS JOIN numbers AS N\n"+
			"	  WHERE writers IS NOT NULL AND CHAR_LENGTH(writers) - CHAR_LENGTH(REPLACE(writers, ',', '')) + 1 >= N.number\n"+
			"),\n"+
			"all_associations AS (\n"+
			"	  SELECT SA.tconst, SA.nconst\n"+
			"	  FROM split_associations AS SA\n"+
			"	UNION\n"+
			"	  SELECT TP.tconst, TP.nconst\n"+
			"	  FROM imdb.title_principals AS TP\n"+
			"	  JOIN titles_for_person AS TFP ON TFP.tconst = TP.tconst\n"+
			"),\n"+
			"other_people AS (\n"+
			"	SELECT nconst\n"+
			"	FROM all_associations\n"+
			"	WHERE nconst != @nconst\n"+
			"),\n"+
			"top_peers AS (\n"+
			"	SELECT OP.nconst, COUNT(*) as common_titles\n"+
			"	FROM other_people AS OP\n"+
			"	GROUP BY nconst\n"+
			"	ORDER BY common_titles DESC\n"+
			"	LIMIT 5\n"+
			")\n"+
			"SELECT TP.nconst, TP.common_titles, NB.*\n"+
			"FROM top_peers AS TP\n"+
			"JOIN imdb.name_basics AS NB ON NB.nconst = TP.nconst\n"+
			"ORDER BY TP.common_titles DESC", sql.Named("nconst", name)).
			Scan(&result)

		return result
	}

	var mostCommonTeammatesInApp = func() []entities.NameBasic {
		var titlesPrincipalMatchingPersonEntities []entities.TitlePrincipal
		db.
			Model(&entities.TitlePrincipal{}).
			Where("nconst = ?", name).
			Find(&titlesPrincipalMatchingPersonEntities)

		var titlesPrincipalMatchingPerson = lo.Map(titlesPrincipalMatchingPersonEntities, func(x entities.TitlePrincipal, index int) string {
			return x.Tconst
		})

		var otherTitlePrincipalsEntities []entities.TitlePrincipal
		db.
			Model(&entities.TitlePrincipal{}).
			Where("nconst != ?", name).
			Where("tconst IN ?", titlesPrincipalMatchingPerson).
			Find(&otherTitlePrincipalsEntities)

		var otherTitlePrincipals = lo.Map(otherTitlePrincipalsEntities, func(x entities.TitlePrincipal, index int) string {
			return x.Nconst
		})

		var titleCrewMatchingPersonEntities []entities.TitleCrew
		db.
			Model(&entities.TitleCrew{}).
			Where("directors LIKE @name OR writers LIKE @name", sql.Named("name", "%"+name+"%")).
			Find(&titleCrewMatchingPersonEntities)

		var titleCrewMatchingPerson = lo.FlatMap(lo.Filter(titleCrewMatchingPersonEntities, func(x entities.TitleCrew, index int) bool {
			return lo.Contains(strings.Split(x.Directors, ","), name) ||
				lo.Contains(strings.Split(x.Writers, ","), name)
		}), func(x entities.TitleCrew, index int) []string {
			return lo.Uniq(append(strings.Split(x.Directors, ","), strings.Split(x.Writers, ",")...))
		})

		var allTeammates = lo.Filter(append(otherTitlePrincipals, titleCrewMatchingPerson...), func(x string, index int) bool {
			return x != "" && x != name
		})

		counts := make(map[string]int)
		for _, teammate := range allTeammates {
			counts[teammate] = counts[teammate] + 1
		}

		var countsWithKeys []lo.Tuple2[string, int]
		for _, key := range lo.Keys(counts) {
			countsWithKeys = append(countsWithKeys, lo.T2(key, counts[key]))
		}

		sort.SliceStable(countsWithKeys, func(i, j int) bool {
			return countsWithKeys[i].B > countsWithKeys[j].B
		})

		topResults := lo.Subset(countsWithKeys, 0, 5)

		var result []entities.NameBasic
		db.
			Model(&entities.NameBasic{}).
			Where("nconst IN ?", lo.Map(topResults, func(x lo.Tuple2[string, int], index int) string {
				return x.A
			})).
			Find(&result)

		return result
	}

	if method == "" { 
		return mostCommonTeammatesManual()
	} else {
		return mostCommonTeammatesInApp()
	}
}
