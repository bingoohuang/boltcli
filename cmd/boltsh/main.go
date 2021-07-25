package main

import (
	"errors"
	"fmt"
	"github.com/bingoohuang/boltcli"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/c-bata/go-prompt"
	"github.com/urfave/cli/v2"
)

var (
	boltCli *boltcli.DB
	cliApp  *cli.App
)

func completer(d prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{
		{Text: "open", Description: "short:[o]; open a boltdb file. e.g: open test.db"},
		{Text: "close", Description: "short:[c]; close boltdb file. e.g: close"},
		{Text: "exit", Description: "short:[x]; exit this shell. e.g: eixt/quit"},
		{Text: "use", Description: "short:[u]; select a bucket. e.g: use bucketname"},
		{Text: "bucket.new", Description: "short:[nb]; create a bucket. e.g: newbucket bucketname"},
		{Text: "bucket.del", Description: "short:[db]; delete a bucket. e.g: deletebucket bucketname"},
		{Text: "bucket.list", Description: "short:[lb]; list buckets in the dbfile. e.g: listbucket"},
		{Text: "set", Description: "short:[s]; set value to key in current bucket. e.g: set keyname value"},
		{Text: "delete", Description: "short:[d]; Delete a key in the `BUCKET`.. e.g: delete keyname"},
		{Text: "get", Description: "short:[g]; get a value by key in current bucket. e.g: get keyname"},
		{Text: "list", Description: "short:[ls]; list all keys and values in the bucket. e.g: list"},
		{Text: "seq.next", Description: "short:[ns]; Get next sequence of current bucket.. e.g: nextsequence"},
		{Text: "seq", Description: "short:[seq]; Get  sequence of current bucket. e.g: sequence"},
		{Text: "seq.set", Description: "short:[ss]; Set sequence of current bucket. e.g: setsequence 123"},
		{Text: "backup", Description: "short:[bak]; create a backup of the boltdb file. e.g: backup"},
		{Text: "show", Description: "short:[sh]; Show parameters of the db. e.g: show"},
		{Text: "stats", Description: "short:[st]; Show stats of the db. e.g: stats"},
		{Text: "help", Description: "short:[h]; Show help information. e.g: help"},
	}
	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}

func main() {
	// 可传递文件名到程序。
	if len(os.Args) > 1 {
		handleCmd("open " + os.Args[1])
	}

	for bRunning := true; bRunning; {
		t := prompt.Input("> ", completer)
		switch t {
		case "quit", "exit":
			closeDB()
			bRunning = false
		default:
			handleCmd(t)
		}
	}

	fmt.Println("bye!")
}

func closeDB() {
	if boltCli != nil {
		boltCli.Close()
		boltCli = nil
	}
}

func isBoltCliReady() bool {
	return boltCli != nil
}

func handleCmd(cmd string) {
	if len(strings.Trim(cmd, "")) == 0 {
		return
	}
	args := strings.Split(cmd, " ")
	args = append([]string{"boltsh"}, args...)

	err := cliApp.Run(args)
	if err != nil {
		fmt.Println(err)
	}
}

func init() {
	cliApp = &cli.App{
		Name: "boltsh", Usage: "a shell for boltdb file", Version: "0.0.1",
		Compiled: time.Now(), EnableBashCompletion: true, HideHelp: false, HideVersion: false,
		Action: func(c *cli.Context) error {
			fmt.Println("Use boltsh --help")
			return nil
		},
	}

	cliApp.Commands = []*cli.Command{
		{Name: "open", Aliases: []string{"o"}, Category: "database", Usage: "Open a boltdb file",
			Action: dbOpen, Flags: []cli.Flag{&cli.BoolFlag{Name: "forever", Aliases: []string{"forevvarr"}}}},
		{Name: "close", Aliases: []string{"c"}, Category: "database", Usage: "Close a boltdb file", Action: dbClose},
		{Name: "backup", Aliases: []string{"bak"}, Category: "database", Usage: "Close a backup of the moltdb file", Action: dbBackup},
		{Name: "use", Aliases: []string{"u"}, Category: "database", Usage: "Switch current bucket", Action: dbUse},
		{Name: "bucket.new", Category: "database", Aliases: []string{"bn"}, Usage: "Create a new bucket. boltCli -f test.db nb newbucket", Action: dbNewBucket},
		{Name: "bucket.del", Aliases: []string{"bd"}, Category: "data", Usage: "Delete a bucket.", Action: dbDeleteBucket},
		{Name: "get", Aliases: []string{"g"}, Category: "data", Usage: "Get value from a key in the `BUCKET`", Action: dbGet, Flags: []cli.Flag{&cli.BoolFlag{Name: "forever", Aliases: []string{"forevvarr"}}}},
		{Name: "list", Aliases: []string{"ls"}, Category: "data", Usage: "List all data in the `BUCKET`.", Action: dbList},
		{Name: "set", Aliases: []string{"s"}, Category: "data", Usage: "Set value by a key in the `BUCKET`.", Action: dbSet},
		{Name: "delete", Aliases: []string{"d"}, Category: "data", Usage: "Delete a key in the `BUCKET`.", Action: dbDelete},
		{Name: "seq.next", Category: "data", Usage: "Get NextSequence of current bucket.", Action: dbNextSeq},
		{Name: "seq", Category: "data", Usage: "Get sequence of current bucket.", Action: dbSequence},
		{Name: "seq.set", Aliases: []string{"ss"}, Category: "data", Usage: "Set sequence of current bucket.", Action: dbSetSequence},
		{Name: "bucket.list", Aliases: []string{"bl"}, Category: "database", Usage: "List all buckets in the db.", Action: dbListBucket},
		{Name: "show", Aliases: []string{"sh"}, Category: "database", Usage: "Show parameters of the db.", Action: dbShow},
		{Name: "stats", Aliases: []string{"st"}, Category: "database", Usage: "short:[st]; Show stats of the db. e.g: stats", Action: dbStats},
	}

	sort.Sort(cli.FlagsByName(cliApp.Flags))
	sort.Sort(cli.CommandsByName(cliApp.Commands))
}

var (
	ErrDbNotOpen     = errors.New("open a boltdb file first. By open command")
	ErrSetSeqNeedNum = errors.New("set sequence need a number")
)

func dbOpen(c *cli.Context) error {
	dbFile := c.Args().First()

	closeDB()

	var err error
	boltCli, err = boltcli.New(dbFile)
	if err != nil {
		return errors.New("new boltCli err " + err.Error())
	}
	fmt.Println(dbFile + " opened")
	return nil
}

func dbClose(c *cli.Context) error {
	if isBoltCliReady() {
		fmt.Println(boltCli.DbFile + " closed")
		closeDB()
	} else {
		fmt.Println("No dbfile is opened.")
	}
	return nil
}

func dbBackup(c *cli.Context) error {
	if isBoltCliReady() {
		date := time.Now().Format("20060102150405")
		filePath := boltCli.DbFile + "-" + date + ".bak"

		err := boltCli.Backup(filePath)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Backup ok: " + filePath)
		}
	} else {
		fmt.Println("No dbfile is opened.")
	}
	return nil
}

func use(bucketName string) {
	boltCli.WithBucket([]byte(bucketName))
}

func dbUse(c *cli.Context) error {
	if !isBoltCliReady() {
		return ErrDbNotOpen
	}

	bucket := c.Args().First()
	use(bucket)

	fmt.Println("Switch to " + bucket)
	return nil
}

func dbGet(c *cli.Context) error {
	if !isBoltCliReady() {
		return ErrDbNotOpen
	}

	key := c.Args().First()
	if key == "" {
		return errors.New("need key")
	}

	v, err := boltCli.Get([]byte(key))
	if err != nil {
		return errors.New("get key err " + err.Error())
	}

	fmt.Printf("get %s.%s=%s\n", boltCli.Bucket, key, string(v))
	return nil
}

func dbList(c *cli.Context) error {
	if !isBoltCliReady() {
		return ErrDbNotOpen
	}

	cnt := 0
	err := boltCli.List(func(index int, key, v []byte) bool {
		cnt = cnt + 1
		fmt.Printf("get %d %s.%s=%s\n", cnt, boltCli.Bucket, key, string(v))
		return true

	})
	if err != nil {
		return errors.New("list bucket err " + err.Error())
	}

	fmt.Println(fmt.Sprintf("Total: %d items.", cnt))

	return nil
}

func dbSet(c *cli.Context) error {
	if !isBoltCliReady() {
		return ErrDbNotOpen
	}

	key, value := c.Args().Get(0), c.Args().Get(1)
	fmt.Printf("set %s.%s=%s\n", boltCli.Bucket, key, value)
	err := boltCli.Put([]byte(key), []byte(value))
	if err != nil {
		return errors.New("Set err " + err.Error())
	}
	return nil
}

func dbDelete(c *cli.Context) error {
	if !isBoltCliReady() {
		return ErrDbNotOpen
	}

	key := c.Args().First()
	err := boltCli.Del([]byte(key))
	if err != nil {
		return errors.New("Delete err " + err.Error())
	}

	fmt.Println("Deleted " + key)
	return nil
}

func dbNextSeq(c *cli.Context) error {
	if !isBoltCliReady() {
		return ErrDbNotOpen
	}

	seq, err := boltCli.NextSeq()
	if err != nil {
		return errors.New("Set err " + err.Error())
	}

	fmt.Println(seq)

	return nil
}

func dbSequence(c *cli.Context) error {
	if !isBoltCliReady() {
		return ErrDbNotOpen
	}

	seq, err := boltCli.Seq()
	if err != nil {
		return errors.New("Set err " + err.Error())
	}

	fmt.Println(seq)

	return nil

}
func dbSetSequence(c *cli.Context) error {
	if !isBoltCliReady() {
		return ErrDbNotOpen
	}

	seq := c.Args().First()
	if len(seq) == 0 {
		return ErrSetSeqNeedNum
	}
	u, _ := strconv.ParseUint(seq, 0, 64)

	err := boltCli.SetSeq(u)
	if err != nil {
		return errors.New("Set err " + err.Error())
	}

	fmt.Println(seq)

	return nil

}

func dbListBucket(c *cli.Context) error {
	if !isBoltCliReady() {
		return ErrDbNotOpen
	}

	bs, err := boltCli.GetBuckets()
	if err != nil {
		return errors.New("GetBuckets err " + err.Error())
	}

	for i, b := range bs {
		fmt.Printf(" %d : %s", i+1, b)
	}

	return nil
}

func dbNewBucket(c *cli.Context) error {
	if !isBoltCliReady() {
		return ErrDbNotOpen
	}

	bucket := c.Args().First()

	if len(bucket) == 0 {
		return errors.New("NewBucket err, need bucket name.")
	}

	boltCli.NewBucket([]byte(bucket))

	fmt.Println("Bucket created: " + bucket)
	return nil
}

func dbDeleteBucket(c *cli.Context) error {
	if !isBoltCliReady() {
		return ErrDbNotOpen
	}

	bucket := c.Args().First()

	if len(bucket) == 0 {
		return errors.New("DeleteBucket err, need bucket name.")
	}

	err := boltCli.DelBucket([]byte(bucket))
	if err != nil {
		return errors.New("DeleteBucket('" + bucket + "') returns err : " + err.Error())
	}

	fmt.Println("Bucket deleted: " + bucket)
	return nil
}

func dbShow(c *cli.Context) error {
	fmt.Printf("Current DB\t: %s \n", boltCli.DbFile)
	fmt.Printf("Current Bucket\t: %s\n", boltCli.Bucket)
	return nil
}

func dbStats(c *cli.Context) error {
	if !isBoltCliReady() {
		return ErrDbNotOpen
	}

	stats, err := boltCli.Stats(boltCli.Bucket)
	if err != nil {
		return errors.New("Stats err " + err.Error())
	}

	fmt.Println("Page count statistics.")
	fmt.Println(fmt.Sprintf("BranchPageN     = %d\t int // number of logical branch pages", stats.BranchPageN))
	fmt.Println(fmt.Sprintf("BranchOverflowN = %d\t int // number of physical branch overflow pages", stats.BranchOverflowN))
	fmt.Println(fmt.Sprintf("LeafPageN       = %d\t int // number of logical leaf pages", stats.LeafPageN))
	fmt.Println(fmt.Sprintf("LeafOverflowN   = %d\t int // number of physical leaf overflow pages", stats.LeafOverflowN))
	fmt.Println("")

	fmt.Println("Tree statistics.")
	fmt.Println(fmt.Sprintf("KeyN            = %d\t int // number of keys/value pairs", stats.KeyN))
	fmt.Println(fmt.Sprintf("Depth           = %d\t int // number of levels in B+tree", stats.Depth))
	fmt.Println("")

	fmt.Println("Page size utilization.")
	fmt.Println(fmt.Sprintf("BranchAlloc     = %d\t int // bytes allocated for physical branch pages", stats.BranchAlloc))
	fmt.Println(fmt.Sprintf("BranchInuse     = %d\t int // bytes actually used for branch data", stats.BranchInuse))
	fmt.Println(fmt.Sprintf("LeafAlloc       = %d\t int // bytes allocated for physical leaf pages", stats.LeafAlloc))
	fmt.Println(fmt.Sprintf("LeafInuse       = %d\t int // bytes actually used for leaf data", stats.LeafInuse))
	fmt.Println("")

	fmt.Println("Bucket statistics")
	fmt.Println(fmt.Sprintf("BucketN           = %d\t int // total number of buckets including the top bucket", stats.BucketN))
	fmt.Println(fmt.Sprintf("InlineBucketN     = %d\t int // total number on inlined buckets", stats.InlineBucketN))
	fmt.Println(fmt.Sprintf("InlineBucketInuse = %d\t int // bytes used for inlined buckets (also accounted for in LeafInuse)", stats.InlineBucketInuse))

	return nil
}
