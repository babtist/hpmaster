package main

import (
    "context"
    "crypto/rsa"
    "crypto/x509"
    "encoding/base64"
    "encoding/json"
    "encoding/pem"
    "fmt"
    "log"
    "math/big"
    "strings"
    "time"

    "github.com/aws/aws-lambda-go/events"
    "github.com/aws/aws-lambda-go/lambda"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/golang-jwt/jwt/v4"
    "github.com/google/uuid"
    "github.com/go-resty/resty/v2"
)

var (
    db                 *dynamodb.DynamoDB
    googleCertsURL     = "https://www.googleapis.com/oauth2/v3/certs"
    usersTableName     = "Users"
    region             = "eu-north-1"
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

type GoogleCert struct {
    Keys []struct {
        Kid string   `json:"kid"`
        Kty string   `json:"kty"`
        Alg string   `json:"alg"`
        Use string   `json:"use"`
        X5c []string `json:"x5c"`
        N   string   `json:"n"`  // Modulus for RSA key
        E   string   `json:"e"`  // Exponent for RSA key
    } `json:"keys"`
}

type CustomClaims struct {
    Email string `json:"email"`
    jwt.RegisteredClaims
}

func HandleRequest(ctx context.Context, event events.APIGatewayCustomAuthorizerRequest) (events.APIGatewayCustomAuthorizerResponse, error) {
    log.Printf("Received Event: %+v", event)

    idToken := event.AuthorizationToken
    if idToken == "" {
        log.Printf("Missing Authorization token")
        return events.APIGatewayCustomAuthorizerResponse{}, fmt.Errorf("missing Authorization token")
    }

    if strings.HasPrefix(idToken, "Bearer ") {
        idToken = idToken[7:]
    }

    log.Printf("Stripped Token: %s", idToken)  // Log stripped token
    segments := strings.Split(idToken, ".")
    if len(segments) != 3 {
        log.Printf("Invalid JWT format, expected 3 segments")
        return events.APIGatewayCustomAuthorizerResponse{}, fmt.Errorf("invalid JWT format, expected 3 segments")
    }

    certs, err := getGoogleCerts()
    if err != nil {
        log.Printf("Error fetching Google certs: %v", err)
        return events.APIGatewayCustomAuthorizerResponse{}, fmt.Errorf("could not fetch Google certs")
    }

    log.Printf("Fetched Google certs: %+v", certs)  // Log the fetched certificates

    claims, err := verifyGoogleToken(idToken, certs)
    if err != nil {
        log.Printf("Error verifying token: %v", err)
        return events.APIGatewayCustomAuthorizerResponse{}, fmt.Errorf("invalid ID token")
    }

    log.Printf("Claims: %+v", claims)  // Log claims to inspect what the token contains
    userEmail := claims.Email
    err = storeUserIfNotExists(userEmail, claims.Subject)
    if err != nil {
        log.Printf("Error storing user: %v", err)
        return events.APIGatewayCustomAuthorizerResponse{}, fmt.Errorf("could not store user in DB")
    }

    return generateAllowPolicy(userEmail), nil
}

func getGoogleCerts() (*GoogleCert, error) {
    client := resty.New()
    resp, err := client.R().Get(googleCertsURL)
    if err != nil {
        return nil, err
    }

    var certs GoogleCert
    err = json.Unmarshal(resp.Body(), &certs)
    if err != nil {
        return nil, err
    }

    return &certs, nil
}

// Helper function to decode base64URL
func base64URLDecode(input string) ([]byte, error) {
    return base64.RawURLEncoding.DecodeString(input)
}

// Parse the RSA public key from the 'n' and 'e' fields
func parseRSAPublicKey(nStr, eStr string) (*rsa.PublicKey, error) {
    nBytes, err := base64URLDecode(nStr)
    if err != nil {
        return nil, fmt.Errorf("failed to decode n: %v", err)
    }

    eBytes, err := base64URLDecode(eStr)
    if err != nil {
        return nil, fmt.Errorf("failed to decode e: %v", err)
    }

    eInt := int(big.NewInt(0).SetBytes(eBytes).Int64())

    pubKey := &rsa.PublicKey{
        N: new(big.Int).SetBytes(nBytes),
        E: eInt,
    }
    return pubKey, nil
}

func verifyGoogleToken(idToken string, certs *GoogleCert) (*CustomClaims, error) {
    token, err := jwt.ParseWithClaims(idToken, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
        kid, ok := token.Header["kid"].(string)
        if !ok {
            return nil, fmt.Errorf("missing kid in token header")
        }

        var publicKey interface{}
        for _, key := range certs.Keys {
            if key.Kid == kid {
                // If X5c is populated, use the X5c cert, otherwise use n and e
                if len(key.X5c) > 0 {
                    publicKey = key.X5c[0]
                    block, _ := pem.Decode([]byte(publicKey.(string)))
                    if block == nil {
                        return nil, fmt.Errorf("failed to parse PEM block containing the public key")
                    }

                    cert, err := x509.ParseCertificate(block.Bytes)
                    if err != nil {
                        return nil, fmt.Errorf("failed to parse certificate: %v", err)
                    }

                    return cert.PublicKey, nil
                } else if key.N != "" && key.E != "" {
                    // Use n and e for RSA
                    return parseRSAPublicKey(key.N, key.E)
                }
            }
        }

        return nil, fmt.Errorf("unable to find matching cert")
    })

    if err != nil {
        return nil, err
    }

    claims, ok := token.Claims.(*CustomClaims)
    if !ok || !token.Valid {
        return nil, fmt.Errorf("invalid token")
    }

    return claims, nil
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


func generateAllowPolicy(userId string) events.APIGatewayCustomAuthorizerResponse {
    return events.APIGatewayCustomAuthorizerResponse{
        PrincipalID:    userId,
        PolicyDocument: generatePolicy("Allow"),
    }
}

func generatePolicy(effect string) events.APIGatewayCustomAuthorizerPolicy {
    return events.APIGatewayCustomAuthorizerPolicy{
        Version:   "2012-10-17",
        Statement: []events.IAMPolicyStatement{
            {
                Effect:   effect,
                Action:   []string{"execute-api:Invoke"},
                Resource: []string{"*"},
            },
        },
    }
}

func main() {
    lambda.Start(HandleRequest)
}

