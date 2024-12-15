package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "strconv"

    "github.com/aws/aws-lambda-go/events"
    "github.com/aws/aws-lambda-go/lambda"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var (
    db             *dynamodb.DynamoDB
    wordsTableName = "Words" // Replace with your table name
    region         = "eu-north-1"
)

func init() {
    sess, err := session.NewSession(&aws.Config{
        Region: aws.String(region),
    })
    if err != nil {
        log.Fatalf("Failed to create AWS session: %v", err)
    }
    db = dynamodb.New(sess)
}

// Word represents a word entry in the DynamoDB table
type Word struct {
    Word      string `json:"word"`
    Correct   string `json:"correct"`
    Incorrect []string `json:"incorrect"`
}

// HandleRequest handles incoming API requests
func HandleRequest(ctx context.Context, event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
    numWordsStr := event.QueryStringParameters["numWords"]
    if numWordsStr == "" {
        return events.APIGatewayProxyResponse{StatusCode: 400, Body: "Missing numWords parameter"}, nil
    }

    numWords, err := strconv.Atoi(numWordsStr)
    if err != nil || numWords <= 0 {
        return events.APIGatewayProxyResponse{StatusCode: 400, Body: "Invalid numWords parameter"}, nil
    }

    words, err := getWords(numWords)
    if err != nil {
        log.Printf("Error retrieving words: %v", err)
        return events.APIGatewayProxyResponse{StatusCode: 500, Body: "Internal server error"}, nil
    }

    responseBody, err := json.Marshal(words)
    if err != nil {
        log.Printf("Error marshalling response: %v", err)
        return events.APIGatewayProxyResponse{StatusCode: 500, Body: "Internal server error"}, nil
    }

    return events.APIGatewayProxyResponse{
        StatusCode: 200,
        Body:       string(responseBody),
    }, nil
}

// getWords retrieves a specified number of words from DynamoDB
func getWords(limit int) ([]Word, error) {
    input := &dynamodb.ScanInput{
        TableName: aws.String(wordsTableName),
        Limit:     aws.Int64(int64(limit)),
    }

    result, err := db.Scan(input)
    if err != nil {
        return nil, fmt.Errorf("failed to scan table: %w", err)
    }

    var words []Word
    err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &words)
    if err != nil {
        return nil, fmt.Errorf("failed to unmarshal words: %w", err)
    }

    return words, nil
}

func main() {
    lambda.Start(HandleRequest)
}

