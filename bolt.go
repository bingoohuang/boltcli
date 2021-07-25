package boltcli

import (
	"bytes"
	"errors"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"os"
	"time"
)

var ErrBucketNotFound = errors.New("bucket not found")

type DB struct {
	DB     *bolt.DB
	DbFile string
	Bucket []byte
}

type Option struct {
	DefaultBucket string
}

type OptionFn func(*Option)

func WithDefaultBucket(v string) OptionFn { return func(o *Option) { o.DefaultBucket = v } }

func New(path string, fns ...OptionFn) (*DB, error) {
	// 在当前目录下打开 my.db 这个文件, 如果文件不存在，将会自动创建
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	option := createOption(fns)
	cli := &DB{DB: db, DbFile: path, Bucket: []byte(option.DefaultBucket)}

	return cli, nil
}

func createOption(fns []OptionFn) Option {
	option := Option{}

	for _, fn := range fns {
		fn(&option)
	}

	if option.DefaultBucket == "" {
		option.DefaultBucket = "default"
	}

	return option
}

func (c *DB) Close() error {
	return c.DB.Close()
}

func (c *DB) Backup(targetPath string) error {
	return c.DB.View(func(tx *bolt.Tx) error {
		return tx.CopyFile(targetPath, 0600)
	})
}

func (c *DB) Stats(bucket []byte) (bolt.BucketStats, error) {
	var stats bolt.BucketStats
	err := c.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketNotFound
		}
		stats = b.Stats()
		return nil
	})
	return stats, err
}

func (c *DB) NextSeq() (uint64, error) {
	var id uint64
	err := c.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(c.Bucket)
		if err != nil {
			return err
		}

		id, err = b.NextSequence()
		return err
	})

	return id, err
}

func (c *DB) Seq() (uint64, error) {
	var id uint64
	err := c.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(c.Bucket)
		if b == nil {
			return ErrBucketNotFound
		}

		id = b.Sequence()
		return nil
	})

	return id, err
}

func (c *DB) SetSeq(num uint64) error {
	return c.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(c.Bucket)
		if err != nil {
			return err
		}

		return b.SetSequence(num)
	})
}

func (c *DB) Get(key []byte) ([]byte, error) {
	var ret []byte
	err := c.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(c.Bucket)
		if b == nil {
			return ErrBucketNotFound
		}
		ret = CloneBytes(b.Get(key))
		return nil
	})

	return ret, err
}

func (c *DB) Put(key, value []byte, more ...[]byte) error {
	return c.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(c.Bucket)
		if err != nil {
			return err
		}

		if err := b.Put(key, value); err != nil {
			return err
		}

		for i := 0; i+1 < len(more); i += 2 {
			if err := b.Put(more[i], more[i+1]); err != nil {
				return err
			}
		}

		return nil
	})
}

func CloneBytes(b []byte) []byte {
	var c = make([]byte, len(b))
	copy(c, b)
	return c
}

func (c *DB) GetBuckets() ([][]byte, error) {
	var ret [][]byte
	err := c.DB.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			ret = append(ret, CloneBytes(name))
			return nil
		})
	})

	return ret, err
}

func (c *DB) NewBucket(bucket []byte) error {
	return c.DB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)
		return err
	})
}

func (c *DB) DelBucket(bucket []byte) error {
	return c.DB.Update(func(tx *bolt.Tx) error { return tx.DeleteBucket(bucket) })
}

func (c *DB) WithBucket(bucket []byte) *DB {
	c.Bucket = bucket
	return c
}

func (c *DB) Del(key []byte) (err error) {
	return c.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(c.Bucket)
		if b == nil {
			return ErrBucketNotFound
		}

		return b.Delete(key)
	})
}

func (c *DB) Range(min, max []byte, f func(index int, k, v []byte) bool) (err error) {
	return c.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(c.Bucket)
		// 如果 bucket 返回为 nil，则说明不存在对应 bucket
		if b == nil {
			return fmt.Errorf("bucket %s is not found", c.Bucket)
		}

		i := 0
		cursor := b.Cursor()
		for k, v := cursor.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = cursor.Next() {
			if !f(i, CloneBytes(k), CloneBytes(v)) {
				break
			}
			i++
		}

		return nil
	})
}
func (c *DB) PrefixList(prefix []byte, f func(index int, k, v []byte) bool) (err error) {
	return c.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(c.Bucket)
		if b == nil {
			return fmt.Errorf("bucket %s is not found", c.Bucket)
		}

		i := 0
		cursor := b.Cursor()
		for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			if !f(i, CloneBytes(k), CloneBytes(v)) {
				break
			}
			i++
		}

		return nil
	})
}

// bucketScan scans nested buckets.
func bucketScan(i *int, parent []byte, b *bolt.Bucket, tx *bolt.Tx, f func(index int, key, val []byte) bool) error {
	if b == nil {
		return ErrBucketNotFound
	}

	cursor := b.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		var p []byte
		if len(parent) > 0 {
			p = append(parent, []byte(":")...)
		}

		p = append(p, k...)

		if v == nil {
			if err := bucketScan(i, p, b.Bucket(k), tx, f); err != nil {
				return err
			}
		} else if !f(*i, p, CloneBytes(v)) {
			break
		}
		*i++
	}
	return nil
}

func (c *DB) List(f func(index int, key, val []byte) bool) error {
	i := 0

	return c.DB.View(func(tx *bolt.Tx) error {
		return bucketScan(&i, nil, tx.Bucket(c.Bucket), tx, f)
	})
}

func IsFileExist(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}

	return false
}
