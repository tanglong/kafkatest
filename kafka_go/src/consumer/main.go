package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

const (
	Offset_Commit_Interval     = 10 * time.Second
	LOG_NUM_TO_HBASE       int = 1024
)

var (
	g_signal        = make(chan os.Signal, 1)
	g_slcPartionMsg = make([]*sarama.ConsumerMessage, 0, LOG_NUM_TO_HBASE) //记录每个partion当前消费到的最新的msg
	g_slcMsgBuffer  = make([]*sarama.ConsumerMessage, 0, LOG_NUM_TO_HBASE)

	g_ptConsumer *consumergroup.ConsumerGroup = nil
)

func CreateConsumerGroup() bool {
	g_slcPartionMsg = make([]*sarama.ConsumerMessage, 0, LOG_NUM_TO_HBASE)
	g_slcMsgBuffer = make([]*sarama.ConsumerMessage, 0, LOG_NUM_TO_HBASE)

	ptCgCfg := consumergroup.NewConfig()
	ptCgCfg.Consumer.Fetch.Default = 1024 * 1024
	ptCgCfg.Offsets.CommitInterval = Offset_Commit_Interval
	ptCgCfg.Offsets.Initial = sarama.OffsetOldest

	slcZookeepers := []string{"10.235.102.193:2181"}
	_, ptCgCfg.Zookeeper.Chroot = kazoo.ParseConnectionString(slcZookeepers[0])

	var consumerErr error
	g_ptConsumer, consumerErr = consumergroup.JoinConsumerGroup("test", []string{"wgame_log"}, slcZookeepers, ptCgCfg)
	if consumerErr != nil {
		fmt.Printf("CreateConsumerGroup consumergroup.JoinConsumerGroup failed:%v\n", consumerErr)
		return false
	}

	return true
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			panic("Bug")
		}
	}()

	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	if !CreateConsumerGroup() {
		return
	}

	defer func() {
		fmt.Printf("quit sleep begin\n")
		time.Sleep(Offset_Commit_Interval * 2)
		fmt.Printf("quit sleep end\n")

		if err := g_ptConsumer.Close(); err != nil {
			fmt.Printf("close consumer failed:%v\n", err)
		}
	}()

	signal.Notify(g_signal, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)
	ptTicker := time.NewTicker(5 * time.Second)
	//i := 0
	for {
		select {
		case sig := <-g_signal:
			fmt.Printf("Signal:%s received\n", sig.String())
			return
		case msg := <-g_ptConsumer.Messages():
			fmt.Printf("msg := <-g_ptConsumer.Messages {%s}\n", msg)
			//i += 1
			//if i == 3 {
			//fmt.Printf("delete msg := <-g_ptConsumer.Messages {%s}\n", msg)
			//g_ptConsumer.CommitUpto(msg)
			//	return
			//}

			g_slcPartionMsg = append(g_slcPartionMsg, msg)
			g_slcMsgBuffer = append(g_slcMsgBuffer, msg)
			if len(g_slcMsgBuffer) >= LOG_NUM_TO_HBASE {
			}
		case err := <-g_ptConsumer.Errors():
			fmt.Printf("kafka consume error:%v\n", err)
			if !RecreateConsumerGroup() {
				return
			}
		case <-ptTicker.C:
			if bRegistered, errReg := g_ptConsumer.InstanceRegistered(); errReg != nil || !bRegistered {
				fmt.Printf("bRegistered:%v, errReg:%v := g_ptConsumer.InstanceRegistered()\n", bRegistered, errReg)
				if !RecreateConsumerGroup() {
					return
				}
			}

			select {
			case msg := <-g_ptConsumer.Messages():
				fmt.Printf("msg := <-g_ptConsumer.Messages2 {%v}\n", msg)

				g_slcPartionMsg = append(g_slcPartionMsg, msg)
				g_slcMsgBuffer = append(g_slcMsgBuffer, msg)
				if len(g_slcMsgBuffer) >= LOG_NUM_TO_HBASE {
				}
			default:
				if len(g_slcMsgBuffer) > 0 {
				}
			}
		}
	}
}

func RecreateConsumerGroup() bool {
	fmt.Printf("RecreateConsumerGroup sleep begin\n")
	//sleep的原因是为了确保偏移量写到zookeeper中
	time.Sleep(Offset_Commit_Interval * 2)
	fmt.Printf("RecreateConsumerGroup sleep end\n")

	for more := true; more; {
		select {
		case err := <-g_ptConsumer.Errors():
			fmt.Printf("RecreateConsumerGroup filter all error:%v\n", err)
		default:
			more = false
		}
	}

	if err := g_ptConsumer.Close(); err != nil {
		fmt.Printf("RecreateConsumerGroup close consumer failed:%v", err)
	}

	bReCreate := false
	for nI := 0; nI < 3; nI++ {
		time.Sleep(5 * time.Second)
		if CreateConsumerGroup() {
			bReCreate = true
			break
		}
	}

	if bReCreate {
		fmt.Printf("RecreateConsumerGroup recreate consumer group succeed\n")
		return true
	} else {
		fmt.Printf("RecreateConsumerGroup recreate consumer group failed\n")
		return false
	}
}
