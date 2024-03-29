package query

// Config :
type Config struct {
	Query []struct {
		Name       string
		CsvPath    string
		OutCsv     string
		IncCol     bool
		HdrNames   []string
		RelaOfCond string
		Cond       []struct {
			Header          string
			Value           any
			ValueType       string
			RelaOfCellValue string
		}
	}
}
