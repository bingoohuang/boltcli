package boltcli

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_BoltCli(t *testing.T) {
	c, err := New("test.bolt")
	assert.Nil(t, err)
	defer c.Close()

	err = c.Put([]byte("name"), []byte("bingoo"))
	assert.Nil(t, err)

	v, err := c.Get([]byte("name"))
	assert.Nil(t, err)
	assert.Equal(t, "bingoo", string(v))

	err = c.Put([]byte("name"), []byte("huang"))
	assert.Nil(t, err)
	v, err = c.Get([]byte("name"))
	assert.Nil(t, err)
	assert.Equal(t, "huang", string(v))
}
