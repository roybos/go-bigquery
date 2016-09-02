package main

import (
	"cloud.google.com/go/bigquery"
	"fmt"
	"github.com/gin-gonic/gin"
	"golang.org/x/net/context"
	"net/http"
	"os"
)

func main() {
	r := gin.Default()
	r.Use(gin.Recovery(), gin.Logger())

	proj := os.Getenv("GCLOUD_PROJECT")
	if proj == "" {
		fmt.Println("GCLOUD_PROJECT environment variable must be set.")
		os.Exit(1)
	}

	r.GET("/data", func(c *gin.Context) {

		query := `SELECT repository_language, COUNT(*) AS count
					FROM [publicdata:samples.github_timeline]
					GROUP BY repository_language
					ORDER BY count DESC`

		rows, err := Query(proj, query)

		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, err)

		} else {
			c.JSON(200, rows)
		}
	})

	r.Run(":8080")
}

// Query returns a slice of the reults of a query.
func Query(proj, q string) ([]bigquery.ValueList, error) {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, proj)
	if err != nil {
		return nil, err
	}

	query := client.Query(q)
	iter, err := query.Read(ctx)
	if err != nil {
		return nil, err
	}

	var rows []bigquery.ValueList

	for iter.Next(ctx) {
		var row bigquery.ValueList
		if err := iter.Get(&row); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	return rows, iter.Err()
}
