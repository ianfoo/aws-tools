// sqsfill sends any number of messages to an SQS queue. For testing purposes.
// You can specify a message body, or read one from a file. The message body
// will have the ID prepended, so that each message body is unique.
//
// Usage of ./sqsfill:
// -b string
//   	Message body template (default "Message_Body")
// -c int
//   	Number of messages to insert (default 1000)
// -f string
//   	Read message body template from file
// -q string
//   	Name of queue to fill
// -r string
//   	Queue region (e.g., "us-east-1", "usw01"
// -serial
//   	Fill queue non-concurrently
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/sqs"
)

const (
	defaultCount = 1000
	interval     = 1000
	sqsBatchSize = 10

	defaultMsgTmpl = "Message_Body"
)

var (
	serialMode bool
	count      int
	auth       aws.Auth
	queueName  string
	region     string

	queue *sqs.Queue

	msgBodyTmpl string
)

func main() {
	if serialMode {
		fillSerially()
	} else {
		fillConcurrent()
	}
}

func fillSerially() {
	total := count
	for count > 0 {
		batchSize := sqsBatchSize
		if count%sqsBatchSize != 0 {
			batchSize = count % sqsBatchSize
		}
		batch := genMessageBatch(batchSize)
		_, err := queue.SendMessageBatch(batch)
		if err != nil {
			fmt.Println("Error sending message batch:", err)
		}
		count -= len(batch)
		if count%1000 == 0 {
			fmt.Println(total-count, "messages sent")
		}
	}
}

type partial struct {
	id    int
	count int
}

func fillConcurrent() {
	countCh := make(chan partial)
	workers := int(math.Ceil(float64(count) / float64(interval)))
	for i := 0; i < workers; i++ {
		numMsgs := interval
		if count-i*interval < interval {
			numMsgs = count - i*interval
		}
		go fillSection(i+1, numMsgs, countCh)
	}
	total := 0
	for i := 0; i < workers; i++ {
		chunk := <-countCh
		total += chunk.count
		fmt.Printf("[worker %02d] sent %d messages; total: %d\n",
			chunk.id, chunk.count, total)
	}
}

func fillSection(id, msgCount int, reportCh chan<- partial) {
	sent := 0
	for sent < msgCount {
		batchSize := sqsBatchSize
		if msgCount-sent < sqsBatchSize {
			batchSize = msgCount - sent
		}
		batch := genMessageBatch(batchSize)
		_, err := queue.SendMessageBatch(batch)
		if err != nil {
			fmt.Println("Error sending message batch:", err)
		}
		sent += len(batch)
	}
	reportCh <- partial{id, sent}
}

func genMessageBatch(batchSize int) []sqs.Message {
	var (
		msgs = make([]sqs.Message, batchSize)
		buf  = new(bytes.Buffer)
	)
	for i := range msgs {
		msgs[i] = genMessage(buf)
	}
	return msgs
}

func genMessage(buf *bytes.Buffer) sqs.Message {
	defer buf.Reset()
	buf.WriteString("ID_")
	buf.WriteString(strconv.FormatInt(time.Now().UnixNano(), 10))
	buf.WriteByte('_')
	buf.WriteString(strconv.Itoa(rand.Intn(1000)))
	mID := buf.String()
	buf.WriteByte(' ')
	buf.WriteString(msgBodyTmpl)
	return sqs.Message{
		MessageId: mID,
		Body:      buf.String(),
	}
}

func init() {
	var msgBodyTmplFile string
	flag.StringVar(&queueName, "q", "", "Name of queue to fill")
	flag.StringVar(&region, "r", "", `Queue region (e.g., "us-east-1", "usw01")`)
	flag.StringVar(&msgBodyTmpl, "b", defaultMsgTmpl, "Message body template")
	flag.StringVar(&msgBodyTmplFile, "f", "", "Read message body template from file")
	flag.IntVar(&count, "c", defaultCount, "Number of messages to insert")
	flag.BoolVar(&serialMode, "serial", false, "Fill queue non-concurrently")
	flag.Parse()
	auth, err := aws.EnvAuth()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if queueName == "" || region == "" {
		flag.Usage()
		os.Exit(1)
	}
	if msgBodyTmpl == "" && msgBodyTmplFile == "" {
		flag.Usage()
		os.Exit(1)
	}
	if msgBodyTmplFile != "" {
		body, err := ioutil.ReadFile(msgBodyTmplFile)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		msgBodyTmpl = string(body)
	}
	region = normalizeRegion(region)
	svc, err := sqs.NewFrom(auth.AccessKey, auth.SecretKey, region)
	if err != nil {
		fmt.Println("Error accessing SQS:", err)
		os.Exit(1)
	}
	queue, err = svc.GetQueue(queueName)
	if err != nil {
		fmt.Printf("Error getting queue %s: %s\n", queueName, err)
		os.Exit(1)
	}
}

func normalizeRegion(region string) string {
	switch {
	case region == "use01" || region == "use":
		return "us-east-1"
	case region == "usw01" || region == "usw" || region == "usw1":
		return "us-west-1"
	case region == "usw02" || region == "usw2":
		return "us-west-2"
	case region == "apn01" || region == "apn":
		return "ap-northeast-1"
	case region == "sae01" || region == "sae":
		return "sa-east-1"
	default:
		return region
	}
}
