package main

import (
	"embed"
	"flag"
	"fmt"
	"github.com/bingoohuang/boltcli"
	"github.com/gin-gonic/gin"
	"io/fs"
	"log"
	"net/http"
	"os"
)

var (
	db     *boltcli.DB
	dbName = os.Getenv("BOLTWEB_DB")
	port   = os.Getenv("BOLTWEB_PORT")
)

func init() {
	if dbName == "" {
		dbName = "bolt.bolt"
	}
	if port == "" {
		port = "8080"
	}
	flag.StringVar(&dbName, "d", dbName, "Name of the database")
	flag.StringVar(&port, "p", port, "Port for the web-ui")
}

func main() {
	flag.Parse()
	args := flag.Args()

	// If non-flag options are included assume bolt db is specified.
	if len(args) > 0 {
		dbName = args[0]
	}

	if dbName == "" {
		log.Printf("\nERROR: Missing boltdb name\n")
		os.Exit(1)
	}

	fmt.Print(" ")
	log.Print("starting boltdb-browser..")

	var err error
	db, err = boltcli.New(dbName)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// OK, we should be ready to define/run web server safely.
	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	r.GET("/", Index)
	r.GET("/buckets", Buckets)
	r.POST("/createBucket", CreateBucket)
	r.POST("/put", Put)
	r.POST("/get", Get)
	r.POST("/deleteKey", DeleteKey)
	r.POST("/deleteBucket", DeleteBucket)
	r.POST("/prefixScan", PrefixScan)
	r.StaticFS("/web", http.FS(sub))

	r.Run(":" + port)
}

//go:embed web
var webFS embed.FS

var sub, _ = fs.Sub(webFS, "web")

func Index(c *gin.Context) {
	c.Redirect(301, "/web/index.html")

}

func CreateBucket(c *gin.Context) {
	bucket := c.PostForm("bucket")
	if bucket == "" {
		c.String(200, "no bucket name | n")
	}

	err := db.NewBucket([]byte(bucket))
	if err != nil {
		c.String(200, err.Error())
		return
	}
	c.String(200, "ok")
}

func DeleteBucket(c *gin.Context) {
	bucket := c.PostForm("bucket")
	if bucket == "" {
		c.String(200, "no bucket name | n")
	}

	err := db.DelBucket([]byte(bucket))
	if err != nil {
		c.String(200, err.Error())
		return
	}
	c.String(200, "ok")
}

func DeleteKey(c *gin.Context) {
	bucket := c.PostForm("bucket")
	key := c.PostForm("key")
	if bucket == "" || key == "" {
		c.String(200, "no bucket name or key | n")
	}

	err := db.WithBucket([]byte(bucket)).Del([]byte(key))
	if err != nil {
		c.String(200, err.Error())
		return
	}

	c.String(200, "ok")
}

func Put(c *gin.Context) {
	bucket := c.PostForm("bucket")
	key := c.PostForm("key")
	if bucket == "" || key == "" {
		c.String(200, "no bucket name or key | n")
	}

	value := c.PostForm("value")
	err := db.WithBucket([]byte(bucket)).Put([]byte(key), []byte(value))
	if err != nil {
		c.String(200, err.Error())
		return
	}

	c.String(200, "ok")
}

func Get(c *gin.Context) {
	bucket := c.PostForm("bucket")
	key := c.PostForm("key")
	if bucket == "" || key == "" {
		c.String(200, "no bucket name or key | n")
	}

	value, err := db.WithBucket([]byte(bucket)).Get([]byte(key))
	if err != nil {
		c.JSON(200, []string{"nok", err.Error()})
		return
	}

	c.JSON(200, []string{"ok", string(value)})
}

type Result struct {
	Result string
	M      map[string]string
}

func PrefixScan(c *gin.Context) {
	res := Result{Result: "nok"}
	m := make(map[string]string)

	bucket := c.PostForm("bucket")
	if bucket == "" {
		res.Result = "no bucket name | n"
		c.JSON(200, res)
	}

	key := c.PostForm("key")
	var err error
	if key == "" {
		err = db.WithBucket([]byte(bucket)).List(func(index int, k, v []byte) bool {
			m[string(k)] = string(v)
			return index < 2000
		})
	} else {
		err = db.WithBucket([]byte(bucket)).PrefixList([]byte(key), func(index int, k, v []byte) bool {
			m[string(k)] = string(v)
			return index < 2000
		})
	}

	if err != nil {
		c.JSON(200, Result{Result: err.Error()})
		return
	}

	c.JSON(200, Result{Result: "ok", M: m})
}

func Buckets(c *gin.Context) {
	var res []string
	buckets, _ := db.GetBuckets()
	for _, b := range buckets {
		res = append(res, string(b))
	}

	c.JSON(200, res)
}
