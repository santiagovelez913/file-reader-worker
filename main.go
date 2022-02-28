package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

type FileReaderConfig struct {
	hasHeaders         bool
	headers            []string
	rowTransformations []RowTransformation
}

//TODO: make it really polimorphic with: https://alexkappa.medium.com/json-polymorphism-in-go-4cade1e58ed1
type RowTransformation struct {
	transformationKey    string
	transformationType   string
	leftSideOfOperation  transformationOperand
	rightSideOfOperation transformationOperand
}

type transformationOperand struct {
	operandType  string
	operandValue string
}

func main() {
	fileDiskLocation := downloadFromS3("testS3Bucket", "testS3Path")
	file, err := os.Open(fileDiskLocation)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	config := getFileReaderConfig()
	csvReader := csv.NewReader(file)
	isFirstRow := true
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if config.hasHeaders && isFirstRow {
			isFirstRow = false
			config.headers = row
		} else {
			handleRow(row, config)
		}
	}
}

//TODO: refactor urgente (usar apuntadores, separar en funciones mas atomicas, tener una funcion de operaciones en vez de un if anidado
func handleRow(row []string, config FileReaderConfig) {
	transformations := make(map[string]string)
	for _, transformation := range config.rowTransformations {
		leftSideValue := getOperandValue(transformation.leftSideOfOperation, row, config.headers, transformations)
		rightSideValue := getOperandValue(transformation.rightSideOfOperation, row, config.headers, transformations)
		transformedValue := transForm(transformation, leftSideValue, rightSideValue)
		transformations[transformation.transformationKey] = transformedValue
	}
	sendToFinalDestinationByBatch(config.headers, row, transformations)
}

func sendToFinalDestinationByBatch(headers []string, row []string, transformations map[string]string) {
	//TODO: implement
}

func transForm(transformation RowTransformation, leftSideValue string, rightSideValue string) string {
	var transformedValue string
	if transformation.transformationType == "concat" {
		transformedValue = leftSideValue + rightSideValue
	} else if transformation.transformationType == "multiply" {
		intLeftSideValue, _ := strconv.Atoi(leftSideValue)
		intRightSideValue, _ := strconv.Atoi(rightSideValue)
		transformedValue = strconv.Itoa(intLeftSideValue * intRightSideValue)
	}
	return transformedValue
}

func getOperandValue(operation transformationOperand, row []string, headers []string, transformations map[string]string) string {
	var returnValue string
	if operation.operandType == "column-name" {
		returnValue = row[findStringPositionInArray(operation.operandValue, headers)]
	} else if operation.operandType == "fixed-value" {
		returnValue = operation.operandValue
	} else if operation.operandType == "other-transformation" {
		returnValue = transformations[operation.operandValue]
	}
	return returnValue
}

func findStringPositionInArray(valueToFind string, arrayToSearch []string) int {
	position := -1
	valueToFind = strings.Trim(valueToFind, " ")
	for index, element := range arrayToSearch {
		element = strings.Trim(element, " ")
		if element == valueToFind {
			position = index
			break
		}
	}
	return position
}

func getFileReaderConfig() FileReaderConfig {
	//TODO: make real implementation (get from dynamoDB in some way)
	config := FileReaderConfig{
		hasHeaders: true,
		rowTransformations: []RowTransformation{
			{
				transformationKey:    "description",
				leftSideOfOperation:  transformationOperand{"column-name", "year"},
				rightSideOfOperation: transformationOperand{"column-name", "car brand"},
				transformationType:   "concat",
			},
			{
				transformationKey:    "cost in cop",
				leftSideOfOperation:  transformationOperand{"column-name", "cost in USD"},
				rightSideOfOperation: transformationOperand{"fixed-value", "4100"},
				transformationType:   "multiply",
			},
			{
				transformationKey:    "new altered description",
				leftSideOfOperation:  transformationOperand{"other-transformation", "description"},
				rightSideOfOperation: transformationOperand{"fixed-value", ", que bonito carro (añadido fijo)"},
				transformationType:   "concat",
			},
		},
	}
	return config
}

func downloadFromS3(s3BucketName string, s3Patth string) string {
	//TODO: real implementation
	return "./tests/resources/coma-separated-with-headers.csv"
}
