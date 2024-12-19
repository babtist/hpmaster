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
	db                  *dynamodb.DynamoDB
	wordsTableName      = "Words"
	usersTableName      = "Users"
	statisticsTableName = "WordStatistics"
	region              = "eu-north-1"
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
	Word      string   `json:"word"`
	Correct   string   `json:"correct"`
	Incorrect []string `json:"incorrect"`
}

type WordResults struct {
	Word      string `json:"word"`
	IsCorrect bool   `json:"isCorrect"`
}

type WordStatistics struct {
	UserId       string  `json:"userId"`
	Word         string  `json:"word"`
	Attempts     int     `json:"attempts"`
	Success      int     `json:"success"`
	SuccessRatio float32 `json:"successRatio"`
}

func HandleRequest(ctx context.Context, event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	method := event.RequestContext.HTTPMethod
	switch method {
	case "GET":
		return handleGetWords(event)
	case "POST":
		return handleResults(event)
	default:
		return events.APIGatewayProxyResponse{StatusCode: 405, Body: "Method Not Allowed"}, nil
	}
}

func handleGetWords(event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	numWordsStr := event.QueryStringParameters["numWords"]
	if numWordsStr == "" {
		numWordsStr = "10"
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

func handleResults(event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var userEmail string
	authorizer := event.RequestContext.Authorizer

	if email, ok := authorizer["email"].(string); ok {
		userEmail = email
	} else if claims, ok := authorizer["claims"].(map[string]interface{}); ok {
		// Handle custom claims (if your Authorizer outputs claims in Payload V2.0)
		if emailClaim, exists := claims["email"].(string); exists {
			userEmail = emailClaim
		} else {
			return events.APIGatewayProxyResponse{StatusCode: 401, Body: "Unauthorized: Email not found"}, nil
		}
	} else {
		return events.APIGatewayProxyResponse{StatusCode: 401, Body: "Unauthorized"}, nil
	}
	log.Printf("User email: %s", userEmail)

	userId, err := getUserIdByEmail(userEmail)
	if err != nil || userId == nil {
		if err != nil {
			log.Printf("Error getting user id: %v", err)
		}
		return events.APIGatewayProxyResponse{StatusCode: 400, Body: "User not found"}, nil
	}

	var wordResults []WordResults
	err = json.Unmarshal([]byte(event.Body), &wordResults)
	if err != nil {
		log.Printf("Invalid request body: %v", err)
		return events.APIGatewayProxyResponse{StatusCode: 400, Body: "Invalid request body"}, nil
	}

	// Process and update each word result
	for _, result := range wordResults {
		err := updateWordStatistics(*userId, result)
		if err != nil {
			log.Printf("Error updating word statistics: %v", err)
			return events.APIGatewayProxyResponse{StatusCode: 500, Body: "Failed to update statistics"}, nil
		}
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       "Word resultss successfully uploaded",
	}, nil
}

func getUserIdByEmail(email string) (*string, error) {
	result, err := db.Query(&dynamodb.QueryInput{
		TableName:              aws.String(usersTableName),
		IndexName:              aws.String("email-userId-index"),
		KeyConditionExpression: aws.String("email = :email"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":email": {
				S: aws.String(email),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(result.Items) == 0 {
		return nil, nil
	}
	userId := result.Items[0]["userId"].S
	return userId, nil
}

func updateWordStatistics(userId string, result WordResults) error {
	// Define the primary key (userId and word)
	key := map[string]*dynamodb.AttributeValue{
		"userId": {S: aws.String(userId)},
		"word":   {S: aws.String(result.Word)},
	}

	// Build the update expression
	updateExpression := "SET attempts = if_not_exists(attempts, :zero) + :inc, " +
		"success = if_not_exists(success, :zero) + :successInc, " +
		"successRatio = (success + :successInc) / (attempts + :inc)"

	// Define the expression attribute values
	expressionValues := map[string]*dynamodb.AttributeValue{
		":zero":       {N: aws.String("0")},
		":inc":        {N: aws.String("1")},
		":successInc": {N: aws.String("1")},
	}
	if !result.IsCorrect {
		expressionValues[":successInc"] = &dynamodb.AttributeValue{N: aws.String("0")}
	}

	// Perform the update
	_, err := db.UpdateItem(&dynamodb.UpdateItemInput{
		TableName:                 aws.String(statisticsTableName),
		Key:                       key,
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: expressionValues,
		ReturnValues:              aws.String("UPDATED_NEW"),
	})
	if err != nil {
		log.Printf("Error updating item: %v", err)
		return err
	}
	return nil
}

func main() {
	lambda.Start(HandleRequest)
}
