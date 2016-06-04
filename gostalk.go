package gostalk

import (
	"net"
	"bufio"
)

type Job struct {
	Id uint64
	Body []byte
}

type Gostalk struct {
	conn net.Conn
	reader *bufio.Reader
}

var ReaderSize = 4096

// Returns a new beanstalkd tcp connection and sets up a new buffered reader
func Connect(addr string) (this *Gostalk, err error) {

	this = new(Gostalk)
	this.conn, err = net.Dial("tcp", addr)
	if err != nil {
		return
	}

	this.reader = bufio.NewReaderSize(this.conn, ReaderSize)

	return
}

// Disconnects from the beanstalkd server
func (this *Gostalk) Disconnect() (err error) {

	err = this.conn.Close();

	return
}
