package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/sqs"
)

const (
	defqueue = "message_queue"
	defattr  = "ApproximateNumberOfMessages"
)

var (
	queueList, attrList string
	queues, attrs       []string
)

func main() {
	auth, err := aws.GetAuth("", "", "", time.Time{})
	check(err)
	s := sqs.New(auth, aws.Regions["us-east-1"])
	for _, qn := range queues {
		q, err := s.GetQueue(qn)
		if err != nil {
			fmt.Printf("error for queue %s: %v\n", qn, err)
			continue
		}
		for _, an := range attrs {
			a, err := q.GetQueueAttributes(an)
			if err != nil {
				fmt.Printf(
					"error getting attribute %s for queue %s: %v\n",
					an, qn, err)
				continue
			}
			fmt.Printf(
				"%s.%s: %s\n",
				qn, a.Attributes[0].Name, a.Attributes[0].Value)
		}
	}
}

func init() {
	flag.StringVar(&queueList, "q", defqueue, "Queue Names (comma-separated)")
	flag.StringVar(&attrList, "a", defattr, "Attribute Names (comma-separated)")
	flag.Parse()

	queues = strings.Split(queueList, ",")
	attrs = strings.Split(attrList, ",")
}

func check(err error) {
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
}
