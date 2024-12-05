package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

// КОНФИГ СТРУКТУРЫ ДЖСОН ФАЙЛА
type Configuration struct {
	Name       string              `json:"name"`
	TupleLimit int                 `json:"tuples_limit"`
	Structure  map[string][]string `json:"structure"`
}

// ПАРСИНГ ДЖСОН
func parseSchema(file string) (Configuration, error) {
	var config Configuration
	data, err := os.ReadFile(file)
	if err != nil {
		return config, err
	}
	err = json.Unmarshal(data, &config)
	if err != nil {
		return config, err
	}
	return config, nil
}

// СОАЗДНИЕ ФАЙЛА ПО СХЕМЕ
func createDatabaseStructure(config Configuration) error {
	dbPath := config.Name
	err := os.MkdirAll(dbPath, 0755)
	if err != nil {
		return err
	}
	for tableName, columns := range config.Structure {
		tablePath := path.Join(dbPath, tableName)
		err := os.MkdirAll(tablePath, 0755)
		if err != nil {
			return err
		}
		csvPath := path.Join(tablePath, "1.csv")
		file, err := os.Create(csvPath)
		if err != nil {
			return err
		}
		writer := csv.NewWriter(file)
		err = writer.Write(columns)
		if err != nil {
			return err
		}
		writer.Flush()
		file.Close()
	}
	return nil
}

// ПОЛУЧЕНИЯ ФААЙЛА
func getNextCSVFile(tablePath string, tableName string, config Configuration, tupleLimit int) (string, error) {
	files, err := os.ReadDir(tablePath)
	if err != nil {
		return "", err
	}
	var csvFiles []string
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".csv") {
			continue
		}
		csvFiles = append(csvFiles, f.Name())
	}
	type typeFileInfo struct {
		Name   string
		Number int
	}
	var fileInfos []typeFileInfo
	for _, f := range csvFiles {
		numStr := strings.TrimSuffix(f, ".csv")
		num, err := strconv.Atoi(numStr)
		if err != nil {
			continue
		}
		fileInfos = append(fileInfos, typeFileInfo{f, num})
	}
	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].Number < fileInfos[j].Number
	})
	for _, fi := range fileInfos {
		filePath := path.Join(tablePath, fi.Name)
		file, err := os.Open(filePath)
		if err != nil {
			return "", err
		}
		defer file.Close()
		reader := csv.NewReader(file)
		records, err := reader.ReadAll()
		if err != nil {
			return "", err
		}
		if len(records) < tupleLimit {
			return filePath, nil
		}
	}
	newNumber := len(fileInfos) + 1
	newFileName := fmt.Sprintf("%d.csv", newNumber)
	newFilePath := path.Join(tablePath, newFileName)
	file, err := os.Create(newFilePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	header := config.Structure[tableName]
	err = writer.Write(header)
	if err != nil {
		return "", err
	}
	writer.Flush()
	return newFilePath, nil
}

// ИНСЕРТ
func insertData(config Configuration, command string) {
	parts := strings.Split(command, "INTO")
	if len(parts) < 2 {
		fmt.Println("невалидный INSERT")
		return
	}
	parts = strings.Split(parts[1], "VALUES")
	if len(parts) < 2 {
		fmt.Println("невалидный INSERT")
		return
	}
	tableName := strings.TrimSpace(strings.Split(parts[0], "(")[0])
	valuesStr := strings.Trim(strings.TrimSpace(parts[1]), "()")
	values := strings.Split(valuesStr, ",")
	for i, val := range values {
		values[i] = strings.TrimSpace(val) // Trim spaces from values
	}
	if _, ok := config.Structure[tableName]; !ok {
		fmt.Printf("таблица %s не существует\n", tableName)
		return
	}
	tablePath := path.Join(config.Name, tableName)
	csvFile, err := getNextCSVFile(tablePath, tableName, config, config.TupleLimit)
	if err != nil {
		fmt.Printf("ошибка получения файла: %v\n", err)
		return
	}
	f, err := os.OpenFile(csvFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("ошибка открытия файла: %v\n", err)
		return
	}
	defer f.Close()
	writer := csv.NewWriter(f)
	err = writer.Write(values)
	if err != nil {
		fmt.Printf("Error writing to CSV file: %v\n", err)
		return
	}
	writer.Flush()
	fmt.Println("добавлено")
}

// СЕЛЕКТ
func selectData(config Configuration, command string) {
	selectFields := "*"
	fromTable := ""
	whereCondition := ""
	parts := strings.Split(command, "FROM")
	if len(parts) < 2 {
		fmt.Println("невалидный SELECT")
		return
	}
	selectPart := strings.TrimSpace(parts[0])
	if strings.HasPrefix(selectPart, "SELECT") {
		selectFields = strings.TrimSpace(strings.TrimPrefix(selectPart, "SELECT"))
	}
	fromAndWhere := strings.TrimSpace(parts[1])
	if strings.Contains(fromAndWhere, "WHERE") {
		fromTable = strings.TrimSpace(strings.Split(fromAndWhere, "WHERE")[0])
		whereCondition = strings.TrimSpace(strings.Split(fromAndWhere, "WHERE")[1])
	} else {
		fromTable = strings.TrimSpace(fromAndWhere)
	}
	if _, ok := config.Structure[fromTable]; !ok {
		fmt.Printf("таблица %s не существует \n", fromTable)
		return
	}
	tablePath := path.Join(config.Name, fromTable)
	files, err := os.ReadDir(tablePath)
	if err != nil {
		fmt.Printf("ошибка чтения дириктории: %v\n", err)
		return
	}
	var rows [][]string
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".csv") {
			continue
		}
		filePath := path.Join(tablePath, f.Name())
		file, err := os.Open(filePath)
		if err != nil {
			fmt.Printf("ошибка открытия файла: %v\n", err)
			continue
		}
		defer file.Close()
		reader := csv.NewReader(file)
		records, err := reader.ReadAll()
		if err != nil {
			fmt.Printf("ошибка чтения файла: %v\n", err)
			continue
		}
		if len(records) > 0 {
			records = records[1:]
		}
		if whereCondition != "" {
			partsWhere := strings.Split(whereCondition, "AND")
			for _, part := range partsWhere {
				part = strings.TrimSpace(part)
				if strings.Contains(part, "OR") {
					orParts := strings.Split(part, "OR")
					for _, orPart := range orParts {
						orPart = strings.TrimSpace(orPart)
						if strings.Contains(orPart, "=") {
							column := strings.TrimSpace(strings.Split(orPart, "=")[0])
							value := strings.TrimSpace(strings.Trim(strings.Split(orPart, "=")[1], "'"))
							columnIndex := -1
							header := config.Structure[fromTable]
							for i, col := range header {
								if col == column {
									columnIndex = i
									break
								}
							}
							if columnIndex == -1 {
								fmt.Printf("колонка %s отсутсвует в таблице %s\n", column, fromTable)
								continue
							}
							var filteredRows [][]string
							for _, row := range records {
								if len(row) > columnIndex && row[columnIndex] == value {
									filteredRows = append(filteredRows, row)
								}
							}
							records = filteredRows
						}
					}
				} else if strings.Contains(part, "=") {
					column := strings.TrimSpace(strings.Split(part, "=")[0])
					value := strings.TrimSpace(strings.Trim(strings.Split(part, "=")[1], "'"))
					columnIndex := -1
					header := config.Structure[fromTable]
					for i, col := range header {
						if col == column {
							columnIndex = i
							break
						}
					}
					if columnIndex == -1 {
						fmt.Printf("колонка %s отсуствет в таблицке %s\n", column, fromTable)
						continue
					}
					var filteredRows [][]string
					for _, row := range records {
						if len(row) > columnIndex && row[columnIndex] == value {
							filteredRows = append(filteredRows, row)
						}
					}
					records = filteredRows
				}
			}
		}
		rows = append(rows, records...)
	}

	fields := strings.Split(selectFields, ",")
	for _, row := range rows {
		var selectedRow []string
		for _, field := range fields {
			field = strings.TrimSpace(field)
			if field == "*" {
				selectedRow = row
				break
			}
			for i, col := range config.Structure[fromTable] {
				if col == field {
					selectedRow = append(selectedRow, row[i])
					break
				}
			}
		}
		fmt.Println(strings.Join(selectedRow, ", "))
	}
}

// ДЕЛИТ
func deleteData(config Configuration, command string) {
	parts := strings.Split(command, "FROM")
	if len(parts) < 2 {
		fmt.Println("неавалидная команда DELETE")
		return
	}
	fromTable := strings.TrimSpace(strings.Split(parts[1], "WHERE")[0])
	whereCondition := strings.TrimSpace(strings.Split(parts[1], "WHERE")[1])
	if _, ok := config.Structure[fromTable]; !ok {
		fmt.Printf("таблица %s не существует\n", fromTable)
		return
	}
	tablePath := path.Join(config.Name, fromTable)
	files, err := os.ReadDir(tablePath)
	if err != nil {
		fmt.Printf("оишбка чтения таблицы: %v\n", err)
		return
	}
	partsWhere := strings.Split(whereCondition, "=")
	if len(partsWhere) != 2 {
		fmt.Println("невалид where")
		return
	}
	column := strings.TrimSpace(partsWhere[0])
	value := strings.TrimSpace(strings.Trim(partsWhere[1], "'"))
	header := config.Structure[fromTable]
	columnIndex := -1
	for i, col := range header {
		if col == column {
			columnIndex = i
			break
		}
	}
	if columnIndex == -1 {
		fmt.Printf("колнка %s не существует %s\n", column, fromTable)
		return
	}
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".csv") {
			continue
		}
		filePath := path.Join(tablePath, f.Name())
		file, err := os.Open(filePath)
		if err != nil {
			fmt.Printf("ошибка открытия файла: %v\n", err)
			continue
		}
		defer file.Close()
		reader := csv.NewReader(file)
		records, err := reader.ReadAll()
		if err != nil {
			fmt.Printf("ошибка чтения файла: %v\n", err)
			continue
		}
		var newRecords [][]string
		if len(records) > 0 {
			newRecords = append(newRecords, records[0])
			for _, row := range records[1:] {
				if len(row) > columnIndex && row[columnIndex] != value {
					newRecords = append(newRecords, row)
				}
			}
		}
		f, err := os.Create(filePath)
		if err != nil {
			fmt.Printf("ошибка создания файла: %v\n", err)
			continue
		}
		defer f.Close()
		writer := csv.NewWriter(f)
		err = writer.WriteAll(newRecords)
		if err != nil {
			fmt.Printf("ошибка записи файла: %v\n", err)
			continue
		}
		writer.Flush()
	}
	fmt.Println("удалено")
}

// МЕЙН
func main() {
	config, err := parseSchema("schema.json")
	if err != nil {
		fmt.Println("ошибка чтения схемы:", err)
		return
	}
	err = createDatabaseStructure(config)
	if err != nil {
		fmt.Println("ошибка создании датабазы:", err)
		return
	}
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("cmd: ")
		scanner.Scan()
		command := scanner.Text()
		if strings.ToUpper(command) == "EXIT" {
			break
		}
		if strings.HasPrefix(strings.ToUpper(command), "INSERT") {
			insertData(config, command)
		} else if strings.HasPrefix(strings.ToUpper(command), "SELECT") {
			selectData(config, command)
		} else if strings.HasPrefix(strings.ToUpper(command), "DELETE") {
			deleteData(config, command)
		} else {
			fmt.Println("неизвестная кмнд")
		}
	}
}
