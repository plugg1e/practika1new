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

// Configuration struct to hold database settings and table structures
type Configuration struct {
	Name       string              `json:"name"`
	TupleLimit int                 `json:"tuples_limit"`
	Structure  map[string][]string `json:"structure"`
}

// Function to parse the JSON schema file
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

// Function to create directories and CSV files based on the schema
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

// Function to get the next CSV file to append to
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

// / Function to handle INSERT command
func insertData(config Configuration, command string) {
	parts := strings.Split(command, "INTO")
	if len(parts) < 2 {
		fmt.Println("Invalid INSERT command")
		return
	}
	parts = strings.Split(parts[1], "VALUES")
	if len(parts) < 2 {
		fmt.Println("Invalid INSERT command")
		return
	}
	tableName := strings.TrimSpace(strings.Split(parts[0], "(")[0])
	valuesStr := strings.Trim(strings.TrimSpace(parts[1]), "()")
	values := strings.Split(valuesStr, ",")
	for i, val := range values {
		values[i] = strings.TrimSpace(val) // Trim spaces from values
	}
	if _, ok := config.Structure[tableName]; !ok {
		fmt.Printf("Table %s does not exist\n", tableName)
		return
	}
	tablePath := path.Join(config.Name, tableName)
	csvFile, err := getNextCSVFile(tablePath, tableName, config, config.TupleLimit)
	if err != nil {
		fmt.Printf("Error getting next CSV file: %v\n", err)
		return
	}
	f, err := os.OpenFile(csvFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Error opening CSV file: %v\n", err)
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
	fmt.Println("Insert successful")
}

// Function to handle SELECT command
func selectData(config Configuration, command string) {
	selectFields := "*"
	fromTable := ""
	whereCondition := ""
	parts := strings.Split(command, "FROM")
	if len(parts) < 2 {
		fmt.Println("Invalid SELECT command")
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
		fmt.Printf("Table %s does not exist\n", fromTable)
		return
	}
	tablePath := path.Join(config.Name, fromTable)
	files, err := os.ReadDir(tablePath)
	if err != nil {
		fmt.Printf("Error reading table directory: %v\n", err)
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
			fmt.Printf("Error opening CSV file: %v\n", err)
			continue
		}
		defer file.Close()
		reader := csv.NewReader(file)
		records, err := reader.ReadAll()
		if err != nil {
			fmt.Printf("Error reading CSV file: %v\n", err)
			continue
		}
		if len(records) > 0 {
			records = records[1:]
		}
		if whereCondition != "" {
			partsWhere := strings.Split(whereCondition, "=")
			if len(partsWhere) != 2 {
				fmt.Println("Invalid WHERE condition")
				continue
			}
			column := strings.TrimSpace(partsWhere[0])
			value := strings.TrimSpace(strings.Trim(partsWhere[1], "'"))
			columnIndex := -1
			header := config.Structure[fromTable]
			for i, col := range header {
				if col == column {
					columnIndex = i
					break
				}
			}
			if columnIndex == -1 {
				fmt.Printf("Column %s does not exist in table %s\n", column, fromTable)
				continue
			}
			var filteredRows [][]string
			for _, row := range records {
				if len(row) > columnIndex && row[columnIndex] == value {
					filteredRows = append(filteredRows, row)
				}
			}
			rows = append(rows, filteredRows...)
		} else {
			rows = append(rows, records...)
		}
	}
	// Use selectFields to determine which columns to print
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

// Function to handle DELETE command
// Function to handle DELETE command
func deleteData(config Configuration, command string) {
	parts := strings.Split(command, "FROM")
	if len(parts) < 2 {
		fmt.Println("Invalid DELETE command")
		return
	}
	fromTable := strings.TrimSpace(strings.Split(parts[1], "WHERE")[0])
	whereCondition := strings.TrimSpace(strings.Split(parts[1], "WHERE")[1])
	if _, ok := config.Structure[fromTable]; !ok {
		fmt.Printf("Table %s does not exist\n", fromTable)
		return
	}
	tablePath := path.Join(config.Name, fromTable)
	files, err := os.ReadDir(tablePath)
	if err != nil {
		fmt.Printf("Error reading table directory: %v\n", err)
		return
	}
	partsWhere := strings.Split(whereCondition, "=")
	if len(partsWhere) != 2 {
		fmt.Println("Invalid WHERE condition")
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
		fmt.Printf("Column %s does not exist in table %s\n", column, fromTable)
		return
	}
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".csv") {
			continue
		}
		filePath := path.Join(tablePath, f.Name())
		file, err := os.Open(filePath)
		if err != nil {
			fmt.Printf("Error opening CSV file: %v\n", err)
			continue
		}
		defer file.Close()
		reader := csv.NewReader(file)
		records, err := reader.ReadAll()
		if err != nil {
			fmt.Printf("Error reading CSV file: %v\n", err)
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
			fmt.Printf("Error creating CSV file: %v\n", err)
			continue
		}
		defer f.Close()
		writer := csv.NewWriter(f)
		err = writer.WriteAll(newRecords)
		if err != nil {
			fmt.Printf("Error writing to CSV file: %v\n", err)
			continue
		}
		writer.Flush()
	}
	fmt.Println("Delete successful")
}

// Main function to run the program
func main() {
	config, err := parseSchema("schema.json")
	if err != nil {
		fmt.Println("Error parsing schema:", err)
		return
	}
	err = createDatabaseStructure(config)
	if err != nil {
		fmt.Println("Error creating database structure:", err)
		return
	}
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("Enter command: ")
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
			fmt.Println("Unknown command")
		}
	}
}
