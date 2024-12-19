package main

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
)

var (
	db             *dynamodb.DynamoDB
	googleCertsURL = "https://www.googleapis.com/oauth2/v3/certs"
	usersTableName = "Users"
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

func HandleRequest(event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	log.Printf("Received Event: %+v", event)

	var userEmail string
	var name string
	authorizer := event.RequestContext.Authorizer

	if email, ok := authorizer["email"].(string); ok {
		userEmail = email
		name = authorizer["given_name"].(string) + " " + authorizer["family_name"].(string)
	} else if claims, ok := authorizer["claims"].(map[string]interface{}); ok {
		// Handle custom claims (if your Authorizer outputs claims in Payload V2.0)
		if emailClaim, exists := claims["email"].(string); exists {
			userEmail = emailClaim
			name = claims["given_name"].(string) + " " + claims["family_name"].(string)
		} else {
			return events.APIGatewayProxyResponse{StatusCode: 401, Body: "Unauthorized: Email not found"}, nil
		}
	} else {
		return events.APIGatewayProxyResponse{StatusCode: 401, Body: "Unauthorized"}, nil
	}

	err := storeUserIfNotExists(userEmail, name)
	if err != nil {
		log.Printf("Error storing user: %v", err)
		return events.APIGatewayProxyResponse{}, fmt.Errorf("could not store user in DB")
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       "",
	}, nil
}

func storeUserIfNotExists(email string, name string) error {
	userId := uuid.New().String()

	user, err := getUserByEmail(email)
	if err != nil {
		log.Printf("Error checking user existence: %v", err)
		return err
	}
	if user != nil {
		return nil
	}

	_, err = db.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(usersTableName),
		Item: map[string]*dynamodb.AttributeValue{
			"userId": {
				S: aws.String(userId),
			},
			"email": {
				S: aws.String(email),
			},
			"name": {
				S: aws.String(name),
			},
			"createdAt": {
				S: aws.String(time.Now().Format(time.RFC3339)),
			},
			"provider": {
				S: aws.String("google"),
			},
		},
	})
	if err != nil {
		log.Printf("Error storing user: %v", err)
		return err
	}
	log.Printf("User %s stored successfully", email)
	return nil
}

func getUserByEmail(email string) (map[string]*dynamodb.AttributeValue, error) {
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
	return result.Items[0], nil // Return the first item (if there are multiple)
}

func main() {
	lambda.Start(HandleRequest)
}
