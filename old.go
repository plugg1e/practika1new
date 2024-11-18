package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Структура для хранения конфигурации
type Schema struct {
	Name        string              `json:"name"`
	TuplesLimit int                 `json:"tuples_limit"`
	Structure   map[string][]string `json:"structure"`
}

// Структура для хранения таблицы
type Table struct {
	Name       string
	Columns    []string
	Rows       [][]string
	PK         int
	PKFileName string
	Lock       sync.Mutex
}

// Функция для чтения конфигурации из файла
func readSchema(filename string) (*Schema, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var schema Schema
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&schema); err != nil {
		return nil, err
	}

	return &schema, nil
}

// Функция для создания директорий и файлов
func createDirectoriesAndFiles(schema *Schema) error {
	schemaDir := schema.Name
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		return err
	}

	for tableName, columns := range schema.Structure {
		tableDir := filepath.Join(schemaDir, tableName)
		if err := os.MkdirAll(tableDir, 0755); err != nil {
			return err
		}

		pkFileName := filepath.Join(tableDir, tableName+"_pk_sequence")
		if _, err := os.Stat(pkFileName); os.IsNotExist(err) {
			if err := os.WriteFile(pkFileName, []byte("0"), 0644); err != nil {
				return err
			}
		}

		lockFileName := filepath.Join(tableDir, tableName+"_Lock")
		if _, err := os.Stat(lockFileName); os.IsNotExist(err) {
			if err := os.WriteFile(lockFileName, []byte(""), 0644); err != nil {
				return err
			}
		}

		csvFileName := filepath.Join(tableDir, "1.csv")
		if _, err := os.Stat(csvFileName); os.IsNotExist(err) {
			file, err := os.Create(csvFileName)
			if err != nil {
				return err
			}
			defer file.Close()

			writer := csv.NewWriter(file)
			defer writer.Flush()

			writer.Write(append([]string{tableName + "_pk"}, columns...))
		}
	}

	return nil
}

// чтения строк из цсв файла
func readRowsFromCSV(filename string) ([][]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return rows, nil
}

//записи строк в цсв файл

func writeRowsToCSV(filename string, rows [][]string) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}

// SELECT
func executeSelect(schema *Schema, query string) error {
	parts := strings.Split(query, " ")
	if len(parts) < 4 || parts[0] != "SELECT" || parts[2] != "FROM" {
		return fmt.Errorf("неверный синтаксис запроса")
	}

	columns := strings.Split(parts[1], ",")
	tableNames := strings.Split(parts[3], ",")

	var rows [][]string
	for _, tableName := range tableNames {
		tableDir := filepath.Join(schema.Name, tableName)
		files, err := os.ReadDir(tableDir)
		if err != nil {
			return err
		}

		for _, file := range files {
			if strings.HasSuffix(file.Name(), ".csv") {
				csvFileName := filepath.Join(tableDir, file.Name())
				tableRows, err := readRowsFromCSV(csvFileName)
				if err != nil {
					return err
				}

				rows = append(rows, tableRows...)
			}
		}
	}

	// обработка выборки колонок
	for _, row := range rows {
		for _, column := range columns {
			colParts := strings.Split(column, ".")
			if len(colParts) != 2 {
				return fmt.Errorf("неверный формат колонки: %s", column)
			}
			tableName := colParts[0]
			colName := colParts[1]

			//индекс колонки
			colIndex := -1
			for i, col := range schema.Structure[tableName] {
				if col == colName {
					colIndex = i + 1 // +1 для учета первичного ключа
					break
				}
			}

			if colIndex == -1 {
				return fmt.Errorf("колонка %s не найдена в таблице %s", colName, tableName)
			}

			fmt.Print(row[colIndex], " ")
		}
		fmt.Println()
	}

	return nil
}

//INSERT

func executeInsert(schema *Schema, query string) error {
	parts := strings.Fields(query)
	if len(parts) < 4 || parts[0] != "INSERT" || parts[1] != "INTO" {
		return fmt.Errorf("неверный синтаксис запроса")
	}

	tableName := parts[2]
	valuesIndex := strings.Index(query, "VALUES")
	if valuesIndex == -1 {
		return fmt.Errorf("неверный синтаксис запроса: отсутствует VALUES")
	}

	values := query[valuesIndex+len("VALUES"):]
	values = strings.Trim(values, "()")
	valueList := strings.Split(values, ",")

	// Удаляем лишние пробелы вокруг значений
	for i := range valueList {
		valueList[i] = strings.TrimSpace(valueList[i])
	}

	// Проверка количества значений
	if len(valueList) != len(schema.Structure[tableName]) {
		return fmt.Errorf("неверное количество значений: ожидалось %d, получено %d", len(schema.Structure[tableName]), len(valueList))
	}

	tableDir := filepath.Join(schema.Name, tableName)
	pkFileName := filepath.Join(tableDir, tableName+"_pk_sequence")
	pkFile, err := os.OpenFile(pkFileName, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer pkFile.Close()

	pkBytes, err := io.ReadAll(pkFile)
	if err != nil {
		return err
	}

	pk, err := strconv.Atoi(string(pkBytes))
	if err != nil {
		return err
	}

	pk++
	if _, err := pkFile.Seek(0, 0); err != nil {
		return err
	}
	if _, err := pkFile.WriteString(strconv.Itoa(pk)); err != nil {
		return err
	}

	files, err := os.ReadDir(tableDir)
	if err != nil {
		return err
	}

	var csvFileName string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".csv") {
			csvFileName = filepath.Join(tableDir, file.Name())
			break
		}
	}

	rows, err := readRowsFromCSV(csvFileName)
	if err != nil {
		return err
	}

	if len(rows) >= schema.TuplesLimit {
		csvFileName = filepath.Join(tableDir, strconv.Itoa(len(files)+1)+".csv")
		file, err := os.Create(csvFileName)
		if err != nil {
			return err
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		defer writer.Flush()

		writer.Write(append([]string{tableName + "_pk"}, schema.Structure[tableName]...))
	}

	rows = append(rows, append([]string{strconv.Itoa(pk)}, valueList...))
	if err := writeRowsToCSV(csvFileName, rows); err != nil {
		return err
	}

	return nil
}

// DELETE
// Функция для выполнения запроса DELETE
func executeDelete(schema *Schema, query string) error {
	parts := strings.Fields(query)
	if len(parts) < 4 || parts[0] != "DELETE" || parts[1] != "FROM" {
		return fmt.Errorf("неверный синтаксис запроса")
	}

	tableName := parts[2]
	whereClause := strings.TrimPrefix(query, "DELETE FROM "+tableName+" WHERE ")

	tableDir := filepath.Join(schema.Name, tableName)
	files, err := os.ReadDir(tableDir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".csv") {
			csvFileName := filepath.Join(tableDir, file.Name())
			rows, err := readRowsFromCSV(csvFileName)
			if err != nil {
				return err
			}

			var newRows [][]string
			for _, row := range rows {
				if !matchesWhereClause(row, whereClause) {
					newRows = append(newRows, row)
				}
			}

			if err := writeRowsToCSV(csvFileName, newRows); err != nil {
				return err
			}
		}
	}

	return nil
}

// Функция для проверки соответствия строки условию WHERE
func matchesWhereClause(row []string, whereClause string) bool {
	parts := strings.Split(whereClause, "=")
	if len(parts) != 2 {
		return false
	}

	column := strings.TrimSpace(parts[0])
	value := strings.TrimSpace(strings.Trim(parts[1], "'"))

	for i, col := range row {
		if strings.TrimSpace(col) == column && strings.TrimSpace(row[i+1]) == value {
			return true
		}
	}

	return false
}

func main() {
	schema, err := readSchema("schema.json")
	if err != nil {
		fmt.Println("Ошибка чтения конфигурации:", err)
		return
	}

	if err := createDirectoriesAndFiles(schema); err != nil {
		fmt.Println("Ошибка создания директорий и файлов:", err)
		return
	}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("Введите запрос: ")
		if !scanner.Scan() {
			break
		}

		query := scanner.Text()
		switch {
		case strings.HasPrefix(query, "SELECT"):
			if err := executeSelect(schema, query); err != nil {
				fmt.Println("Ошибка выполнения запроса SELECT:", err)
			}
		case strings.HasPrefix(query, "INSERT"):
			if err := executeInsert(schema, query); err != nil {
				fmt.Println("Ошибка выполнения запроса INSERT:", err)
			}
		case strings.HasPrefix(query, "DELETE"):
			if err := executeDelete(schema, query); err != nil {
				fmt.Println("Ошибка выполнения запроса DELETE:", err)
			}
		default:
			fmt.Println("Неизвестный запрос")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Ошибка ввода:", err)
	}
}
