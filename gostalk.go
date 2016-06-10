package gostalk

import (
	"net"
	"bufio"
	"fmt"
	"errors"
	"strings"
	"io"
	//"net/textproto"
	"time"
)

type Job struct {
	Id   uint64
	Body []byte
}

type Gostalk struct {
	conn   net.Conn
	reader *bufio.Reader
}

var ReaderSize = 65535

// Sends a formatted command to beanstalkd and returns the received response
func (this *Gostalk) sendReceive(format string, args ...interface{}) (res string, err error) {

	err = this.send(format, args...)
	if err != nil {

		return
	}

	res, err = this.receiveLine()
	res = strings.TrimSuffix(res, "\r\n")

	return
}

func (this *Gostalk) send(format string, args ...interface{}) (err error) {

	_, err = fmt.Fprintf(this.conn, format, args...)
	if err != nil {
		return
	}

	return
}

func (this *Gostalk) receiveLine() (res string, err error) {

	res, err = this.reader.ReadString('\n')

	res = strings.TrimSuffix(res, "\r\n")

	return
}

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

	err = this.send("quit\r\n")
	if err != nil {
		return
	}

	err = this.conn.Close();
	if err != nil {
		return
	}

	return
}

// Put is is for any process that wants to insert a job into the queue.
// It takes the data, priority, delay, time to release
func (this *Gostalk) Put(data []byte, priority uint32, delay time.Duration, ttr time.Duration) (uint64, error) {

	dataLen := len(data)
	res, err := this.sendReceive("put %d %d %d %d\r\n%s\r\n", priority, int(delay.Seconds()), int(ttr.Seconds()), dataLen, data)
	if err != nil {
		return 0, err
	}

	var jobId uint64
	_, err = fmt.Sscanf(res, "INSERTED %d", &jobId)
	if err != nil {
		return 0, err
	}

	return jobId, nil
}

// Reserve waits for a job indefinitely from the server.
// Once the job has been received, it returns the job
// This is a blocking call.
func (this *Gostalk) Reserve() (*Job, error) {

	res, err := this.sendReceive("reserve\r\n")
	if err != nil {
		return nil, err
	}

	var jobId uint64
	var dataLen uint32
	_, err = fmt.Sscanf(res, "RESERVED %d %d", &jobId, &dataLen)
	if err != nil {
		return nil, err
	}

	body, err := this.handleReserveBody(dataLen)
	if err != nil {
		return nil, err
	}

	job := new(Job)
	job.Id = jobId
	job.Body = body[:len(body) - 2]

	return job, nil
}

func (this *Gostalk) ReserveWithTimeout(duration time.Duration) (*Job, error) {

	res, err := this.sendReceive("reserve-with-timeout %d\r\n", int(duration.Seconds()))
	if err != nil {
		return nil, err
	}

	var jobId uint64
	var dataLen uint32
	_, err = fmt.Sscanf(res, "RESERVED %d %d", &jobId, &dataLen)
	if err != nil {
		if res == "TIMED_OUT" {
			return nil, errors.New(res)
		}
		return nil, err
	}

	body, err := this.handleReserveBody(dataLen)
	if err != nil {
		return nil, err
	}

	job := new(Job)
	job.Id = jobId
	job.Body = body

	return job, nil
}

func (this *Gostalk) handleReserveBody(dataLen uint32) ([]byte, error) {
	//read job body
	body := make([]byte, dataLen + 2) //+2 is for trailing \r\n
	_, err := io.ReadFull(this.reader, body)
	if err != nil {
		return nil, err
	}

	return body[:dataLen], nil
}

// Delete a job by it's id
func (this *Gostalk) Delete(jobId uint64) (err error) {

	res, err := this.sendReceive("delete %d\r\n", jobId)
	if err != nil {
		return
	}

	if res != "DELETED" {
		err = errors.New(fmt.Sprintf("Expected (DELETED), GOT %s", res))
	}

	return
}

// Use specifies the tube to use. If the tube does not exist, it will be created.
func (this *Gostalk) Use(tubeName string) (err error) {

	res, err := this.sendReceive("use %s\r\n", tubeName)
	if err != nil {
		return
	}

	var tubeUsed string
	_, err = fmt.Sscanf(res, "USING %s", &tubeUsed)

	return
}

// Sends the watch command to beanstalkd and returns the number of tubes being watched
func (this *Gostalk) Watch(tubeName string) (int, error) {

	res, err := this.sendReceive("watch %s\r\n", tubeName)
	if err != nil {
		return -1, err
	}

	var tubeCount int
	_, err = fmt.Sscanf(res, "WATCHING %d", &tubeCount)
	if err != nil {
		return -1, err
	}

	return tubeCount, nil
}

// Ignores the specified tube and returns the number of tubes being watched.
// Cannot ignore a tube if it's the only tube being watched
func (this *Gostalk) Ignore(tubeName string) (int, error) {

	res, err := this.sendReceive("ignore %s\r\n", tubeName)
	if err != nil {
		return -1, err
	}

	var tubeCount int
	_, err = fmt.Sscanf(res, "WATCHING %d", &tubeCount)
	if err != nil {
		if res == "NOT_IGNORED" {
			return -1, errors.New(fmt.Sprintf("Tube (%s) NOT_IGNORED", tubeName))
		}
		return -1, err
	}

	return tubeCount, nil
}

// Bury a job by it's id
func (this *Gostalk) Bury(jobId uint64, priority uint32) (err error) {

	res, err := this.sendReceive("bury %d %d\r\n", jobId, priority)

	if err != nil {
		return
	}

	expectedRes := "BURIED"
	if res != expectedRes {
		err = errors.New(fmt.Sprintf("Expected (%s), Got (%s)", expectedRes, res))
	}

	return
}
