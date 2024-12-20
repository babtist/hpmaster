package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var (
	db                 *dynamodb.DynamoDB
	wordsTableName     = "Words"
	usersTableName     = "Users"
	wordStatsTableName = "WordStatistics"
	region             = "eu-north-1"

	userCache      map[string]string // In-memory cache for users (email->userId)
	userCacheMutex sync.Mutex        // Mutex to protect userCache
	cachedWords    map[string]Word
	once           sync.Once
	initErr        error
)

func init() {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		log.Fatalf("Failed to create AWS session: %v", err)
	}
	db = dynamodb.New(sess)

	userCache = make(map[string]string)
	cachedWords = make(map[string]Word)

	words, err := fetchWordsFromDynamoDB()
	if err != nil {
		initErr = fmt.Errorf("Initialization error", err)
		return
	}
	if len(words) == 0 {
		initErr = fmt.Errorf("Failed to initialize the cache, no words available")
	}
	for _, word := range words {
		cachedWords[word.Word] = word
	}

}

type User struct {
	UserId    string `json:"userId"`
	Email     string `json:"email`
	CreatedAt string `json:"createdAt"`
	Name      string `json:"name"`
	Provider  string `json:"provider"`
}

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
	if initErr != nil {
		log.Fatalf("Initialization failed: %v", initErr)
	}
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

// Shall only be called from init(). Not protected by mutex
func fetchWordsFromDynamoDB() ([]Word, error) {

	// Query your Words table here
	input := &dynamodb.ScanInput{
		TableName: aws.String(wordsTableName),
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

func handleGetWords(event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	userEmail, err := extractEmail(event)
	if err != nil {
		return events.APIGatewayProxyResponse{StatusCode: 401, Body: fmt.Sprint("%v", err)}, nil
	}

	numWordsStr := event.QueryStringParameters["numWords"]
	if numWordsStr == "" {
		numWordsStr = "10"
	}

	numWords, err := strconv.Atoi(numWordsStr)
	if err != nil || numWords <= 0 {
		return events.APIGatewayProxyResponse{StatusCode: 400, Body: "Invalid numWords parameter"}, nil
	}

	userId, err := getUserIdByEmail(*userEmail)
	if err != nil || userId == nil {
		if err != nil {
			log.Printf("Error getting user id: %v", err)
		}
		return events.APIGatewayProxyResponse{StatusCode: 400, Body: "User not found"}, nil
	}

	words, err := getWords(*userId, numWords)
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

func getPoorPerformanceWords(userID string, limit int) ([]Word, error) {
	// Query for poor performance words (userId = :userId) from WordStatistics table
	performanceInput := &dynamodb.QueryInput{
		TableName:              aws.String(wordStatsTableName),
		IndexName:              aws.String("userId-successRatio-index"), // GSI on userId and successRatio
		KeyConditionExpression: aws.String("userId = :userId"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":userId": {S: aws.String(userID)},
		},
		ScanIndexForward: aws.Bool(true),              // Sort in descending order (poorest first)
		Limit:            aws.Int64(int64(limit / 2)), // Limit to half of the requested limit for poor performance words
	}

	performanceResult, err := db.Query(performanceInput)
	if err != nil {
		return nil, fmt.Errorf("failed to query performance: %w", err)
	}

	// Extract words from the result
	var poorPerformanceWords []string
	for _, item := range performanceResult.Items {
		var wp WordStatistics
		err = dynamodbattribute.UnmarshalMap(item, &wp)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal poor performance word: %w", err)
		}
		poorPerformanceWords = append(poorPerformanceWords, wp.Word)
	}

	// Fetch the complete Word objects from the Words table
	var allPoorPerformanceWords []Word
	for _, word := range poorPerformanceWords {
		if completeWord, exists := cachedWords[word]; exists {
			allPoorPerformanceWords = append(allPoorPerformanceWords, completeWord)
		}
	}

	return allPoorPerformanceWords, nil
}

// Fetch random words
func getRandomWords(limit int) []Word {

	var randomWords []Word

	// Initialize the random number generator
	rand.Seed(time.Now().UnixNano())

	// Initialize the reservoir to hold the first 'limit' words
	i := 0
	for _, word := range cachedWords {
		if i < limit {
			// Fill the reservoir with the first 'limit' words
			randomWords = append(randomWords, word)
		} else {
			// Randomly replace an element in the reservoir with the new word
			r := rand.Intn(i + 1)
			if r < limit {
				randomWords[r] = word
			}
		}
		i++
	}

	return randomWords
}

func getWords(userID string, limit int) ([]Word, error) {
	// Step 1: Fetch Poor Performance Words (with word details)
	poorPerformanceWords, err := getPoorPerformanceWords(userID, limit)
	if err != nil {
		return nil, err
	}

	// Step 2: Check if we have enough poor performance words
	allWords := make([]Word, 0, limit)
	seenWords := make(map[string]bool) // Map to track unique words

	// Add poor performance words first
	for _, word := range poorPerformanceWords {
		if _, exists := seenWords[word.Word]; !exists {
			allWords = append(allWords, word)
			seenWords[word.Word] = true
		}
	}

	// Step 3: If we don't have enough words, fetch random words
	if len(allWords) < limit {
		randomWords := getRandomWords(limit - len(allWords))

		for _, word := range randomWords {
			if _, exists := seenWords[word.Word]; !exists {
				allWords = append(allWords, word)
				seenWords[word.Word] = true
			}
		}
	}

	// Limit the result to the specified number of words
	if len(allWords) > limit {
		allWords = allWords[:limit]
	}

	return allWords, nil
}

func handleResults(event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

	userEmail, err := extractEmail(event)
	if err != nil {
		return events.APIGatewayProxyResponse{StatusCode: 401, Body: fmt.Sprint("%v", err)}, nil
	}

	userId, err := getUserIdByEmail(*userEmail)
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

func extractEmail(event events.APIGatewayProxyRequest) (*string, error) {
	var userEmail string
	authorizer := event.RequestContext.Authorizer

	if email, ok := authorizer["email"].(string); ok {
		userEmail = email
	} else if claims, ok := authorizer["claims"].(map[string]interface{}); ok {
		// Handle custom claims (if your Authorizer outputs claims in Payload V2.0)
		if emailClaim, exists := claims["email"].(string); exists {
			userEmail = emailClaim
		} else {
			return nil, errors.New("Unauthorized: Email not found")
		}
	} else {
		return nil, errors.New("Unauthorized")
	}
	return &userEmail, nil

}

func getUserIdByEmail(email string) (*string, error) {
	if userId, exists := userCache[email]; exists {
		return &userId, nil // Return cached user
	}

	userCacheMutex.Lock()
	defer userCacheMutex.Unlock()
	// Check again after acquiring the lock (double-check locking)
	// It's possible another goroutine already fetched the user in the meantime.
	if userId, exists := userCache[email]; exists {
		return &userId, nil // Return cached user (after the lock)
	}

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
		return nil, errors.New("No user found")
	}
	userId := result.Items[0]["userId"].S
	userCache[email] = *userId
	return userId, nil
}

func updateWordStatistics(userId string, result WordResults) error {
	// Define the primary key (userId and word)
	key := map[string]*dynamodb.AttributeValue{
		"userId": {S: aws.String(userId)},
		"word":   {S: aws.String(result.Word)},
	}

	var wordStats WordStatistics

	resultItem, err := db.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(wordStatsTableName),
		Key:       key,
	})
	if err != nil {
		log.Printf("Error getting WordStatistics: %v", err)
		return err
	}

	if resultItem.Item != nil {
		err = dynamodbattribute.UnmarshalMap(resultItem.Item, &wordStats)
		if err != nil {
			log.Printf("Error unmarshalling result: %v", err)
			return err
		}
	} else {
		wordStats = WordStatistics{
			Word:         result.Word,
			UserId:       userId,
			Attempts:     0,
			Success:      0,
			SuccessRatio: 0,
		}
	}
	wordStats.Attempts++
	if result.IsCorrect {
		wordStats.Success++
	}
	wordStats.SuccessRatio = float32(wordStats.Success) / float32(wordStats.Attempts)

	// Build the update expression
	updateExpression := "SET attempts = :attempts, " +
		"success = :success, " +
		"successRatio = :successRatio"

	// Define the expression attribute values
	expressionValues := map[string]*dynamodb.AttributeValue{
		":attempts":     {N: aws.String(fmt.Sprintf("%d", wordStats.Attempts))},
		":success":      {N: aws.String(fmt.Sprintf("%d", wordStats.Success))},
		":successRatio": {N: aws.String(fmt.Sprintf("%f", wordStats.SuccessRatio))},
	}

	// Perform the update
	_, err = db.UpdateItem(&dynamodb.UpdateItemInput{
		TableName:                 aws.String(wordStatsTableName),
		Key:                       key,
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: expressionValues,
		ReturnValues:              aws.String("UPDATED_NEW"),
	})
	if err != nil {
		log.Printf("Error updating WordStatistics: %v", err)
		return err
	}
	return nil
}

func main() {
	lambda.Start(HandleRequest)
}
