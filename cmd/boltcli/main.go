package main

import (
	"fmt"
	"github.com/bingoohuang/boltcli"
	"os"
	"sort"
	"time"

	"github.com/urfave/cli/v2"
)

var dbfile string
var bucket string

func main() {
	app := &cli.App{
		Name:                 "boltcli",               // 应用名称
		Usage:                "a cli for boltdb file", // 应用功能说明
		Compiled:             time.Now(),
		EnableBashCompletion: true,
		HideHelp:             false,
		HideVersion:          false,
		Action: func(c *cli.Context) error {
			fmt.Println("Use boltcli --help")
			return nil
		},
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{Name: "lang, l", Value: "english", Usage: "Language for the greeting"},
		&cli.StringFlag{Name: "file, f", Usage: "Load db from `FILE`", Value: "test.bolt",
			Destination: &dbfile, Aliases: []string{"f"}},
		&cli.StringFlag{Name: "bucket, b", Usage: "Specify `BUCKET` name.", Value: "default",
			Destination: &bucket, Aliases: []string{"b"}},
	}
	app.Commands = []*cli.Command{
		{Name: "get", Aliases: []string{"g"}, Category: "data", Usage: "Get value from a key in the `BUCKET`", Action: dbGet,
			Flags: []cli.Flag{&cli.BoolFlag{Name: "forever", Aliases: []string{"forevvarr"}}}},
		{Name: "list", Aliases: []string{"ls"}, Category: "data", Usage: "List all data in the `BUCKET`.", Action: dbList},
		{Name: "set", Aliases: []string{"s"}, Category: "data", Usage: "Set value by a key in the `BUCKET`.", Action: dbSet},
		{Name: "bucket.list", Aliases: []string{"bl"}, Category: "bucket", Usage: "List all buckets in the db.", Action: bucketList},
		{Name: "bucket.new", Aliases: []string{"bn"}, Category: "bucket", Usage: "Create a new bucket. boltcli -f test.db nb newbucket", Action: dbNewBucket},
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
	}
}

func dbGet(c *cli.Context) error {
	if !boltcli.IsFileExist(dbfile) {
		return cli.Exit("Db file is not exists: "+dbfile, 1)
	}

	key := c.Args().First()
	if len(key) == 0 {
		return cli.Exit("need key", 1)
	}

	cmd, err := boltcli.New(dbfile)
	if err != nil {
		return cli.Exit("new boltcli err "+err.Error(), 1)
	}
	defer cmd.Close()

	v, err := cmd.WithBucket([]byte(bucket)).Get([]byte(key))
	if err != nil {
		return cli.Exit("get key err "+err.Error(), 1)
	}

	fmt.Println(string(v))

	return nil
}

func dbList(c *cli.Context) error {
	if !boltcli.IsFileExist(dbfile) {
		return cli.Exit("Db file is not exists: "+dbfile, 1)
	}

	cmd, err := boltcli.New(dbfile)
	if err != nil {
		return cli.Exit("new boltcli err "+err.Error(), 1)
	}
	defer cmd.Close()

	err = cmd.WithBucket([]byte(bucket)).List(func(index int, key, val []byte) bool {
		fmt.Printf("%s\t : %s\n", key, val)
		return true
	})
	if err != nil {
		return cli.Exit("list bucket err "+err.Error(), 1)
	}

	return nil
}

func dbSet(c *cli.Context) error {
	key := c.Args().First()
	value := c.Args().Get(1)
	fmt.Println(key, value)

	cmd, err := boltcli.New(dbfile)
	if err != nil {
		return cli.Exit("new boltcli err "+err.Error(), 1)
	}
	defer cmd.Close()

	err = cmd.WithBucket([]byte(bucket)).Put([]byte(key), []byte(value))
	if err != nil {
		return cli.Exit("Set err "+err.Error(), 1)
	}
	return nil
}

func bucketList(c *cli.Context) error {
	if !boltcli.IsFileExist(dbfile) {
		return cli.Exit("Db file is not exists: "+dbfile, 1)
	}

	cmd, err := boltcli.New(dbfile)
	if err != nil {
		return cli.Exit("new boltcli err "+err.Error(), 1)
	}
	defer cmd.Close()

	bs, err := cmd.GetBuckets()
	if err != nil {
		return cli.Exit("GetBuckets err "+err.Error(), 1)
	}

	for _, b := range bs {
		fmt.Printf("%s\n", b)
	}

	return nil
}

func dbNewBucket(c *cli.Context) error {
	bucket := c.Args().First()
	if len(bucket) == 0 {
		return cli.Exit("NewBucket err, need bucket name.", 1)
	}

	cmd, err := boltcli.New(dbfile)
	if err != nil {
		return cli.Exit("new boltcli err "+err.Error(), 1)
	}
	defer cmd.Close()

	err = cmd.NewBucket([]byte(bucket))
	if err != nil {
		return cli.Exit("NewBucket err "+err.Error(), 1)
	}

	fmt.Println("Bucket created: " + bucket)
	return nil
}
