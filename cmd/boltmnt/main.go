package main

import (
	"flag"
	"fmt"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
	bolt "go.etcd.io/bbolt"
	"log"
	"os"
	"path/filepath"
)

var progName = filepath.Base(os.Args[0])

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", progName)
	fmt.Fprintf(os.Stderr, "  %s DBPATH MOUNTPOINT\n", progName)
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "  DBPATH will be created if it does not exist.\n")
	fmt.Fprintf(os.Stderr, "\n")
	flag.PrintDefaults()
}

func main() {
	log.SetFlags(0)
	log.SetPrefix(progName + ": ")

	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 2 {
		usage()
		os.Exit(2)
	}

	err := mount(flag.Arg(0), flag.Arg(1))
	if err != nil {
		log.Fatal(err)
	}
}

type FS struct {
	db *bolt.DB
}

var _ = fs.FS(&FS{})

func (f *FS) Root() (fs.Node, error) {
	n := &Dir{
		fs: f,
	}
	return n, nil
}

func mount(dbpath, mountpoint string) error {
	db, err := bolt.Open(dbpath, 0600, nil)
	if err != nil {
		return err
	}

	c, err := fuse.Mount(mountpoint)
	if err != nil {
		return err
	}
	defer c.Close()

	filesys := &FS{
		db: db,
	}
	if err := fs.Serve(c, filesys); err != nil {
		return err
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		return err
	}

	return nil
}
