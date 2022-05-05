package main

import "C"
import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

type Engine struct{}

type order struct {
	in   input
	time int64
}

type instrumentOrder struct {
	dataStream chan order
}

var instrumentMap = make(map[string]*instrumentOrder)

func remove(slice []input, s int) []input {
	return append(slice[:s], slice[s+1:]...)
}

func instrumentHandler(dataStream chan order) {
	var buyOrders = []input{}
	var sellOrders = []input{}

	for order := range dataStream {
		switch order.in.orderType {
		case inputBuy:
			handleBuy(&order.in, &sellOrders, &buyOrders, order.time)

		case inputSell:
			handleSell(&order.in, &sellOrders, &buyOrders, order.time)

		case inputCancel:
			handleCancel(&order.in, &sellOrders, &buyOrders, order.time)
		}
	}
}

func handleBuy(in *input, sellOrders *[]input, buyOrders *[]input, time int64) {
	var matched bool = false
	var remainingCount = in.count
	var removedCount int = 0 // since we removed some input, we dont want to access outside of slice
	for i, s := range *sellOrders {
		if s.price <= in.price {
			if s.count > remainingCount {
				s.count -= remainingCount
				s.executionID++
				oldRemainingCount := remainingCount
				remainingCount = 0
				matched = true
				//output the order execute
				outputOrderExecuted(s.orderId, in.orderId, s.executionID, s.price, oldRemainingCount, time, GetCurrentTimestamp())
				(*sellOrders)[i-removedCount] = s
				break

			} else {
				remainingCount -= s.count
				s.executionID++
				matched = true
				outputOrderExecuted(s.orderId, in.orderId, s.executionID, s.price, s.count, time, GetCurrentTimestamp())
				*sellOrders = remove(*sellOrders, i-removedCount)
				removedCount++
				continue
			}
		}
		if remainingCount <= 0 {
			break
		}
	}
	if remainingCount > 0 {
		in.count = remainingCount
		*buyOrders = append(*buyOrders, *in)
		//TODO insert order ID of s in something
		if matched {
			outputOrderAdded(*in, time, GetCurrentTimestamp())
		}
	}

	if !matched {
		outputOrderAdded(*in, time, GetCurrentTimestamp())
	}
}

func handleSell(in *input, sellOrders *[]input, buyOrders *[]input, time int64) {
	var matched bool = false
	var remainingCount = in.count // since we removed some input, we dont want to access outside of slice
	var removedCount int = 0
	for i, s := range *buyOrders {
		if s.price >= in.price {
			if s.count > remainingCount {
				s.count -= remainingCount
				s.executionID++
				oldRemainingCount := remainingCount
				remainingCount = 0
				matched = true
				outputOrderExecuted(s.orderId, in.orderId, s.executionID, s.price, oldRemainingCount, time, GetCurrentTimestamp())
				(*buyOrders)[i-removedCount] = s
				break
			} else {
				remainingCount -= s.count
				s.executionID++
				matched = true
				outputOrderExecuted(s.orderId, in.orderId, s.executionID, s.price, s.count, time, GetCurrentTimestamp())
				*buyOrders = remove(*buyOrders, i-removedCount)
				removedCount++
				continue
			}
		}

		if remainingCount <= 0 {
			break
		}
	}
	if remainingCount > 0 {
		in.count = remainingCount
		*sellOrders = append(*sellOrders, *in)

		if matched {
			outputOrderAdded(*in, time, GetCurrentTimestamp())
		}
	}

	if !matched {
		outputOrderAdded(*in, time, GetCurrentTimestamp())
	}
}

func handleCancel(in *input, sellOrders *[]input, buyOrders *[]input, time int64) {
	var targetId = in.orderId
	// search in lists and remove when we meet it
	var found bool = false

	for i, in := range *buyOrders {
		if in.orderId == targetId {
			found = true
			*buyOrders = append((*buyOrders)[:i], (*buyOrders)[i+1:]...)
			outputOrderDeleted(in, true, time, GetCurrentTimestamp())
			return
		}
	}
	for i, in := range *sellOrders {
		if in.orderId == targetId {
			found = true
			*sellOrders = append((*sellOrders)[:i], (*sellOrders)[i+1:]...)
			outputOrderDeleted(in, true, time, GetCurrentTimestamp())
			return
		}
	}
	if found == false {
		outputOrderDeleted(*in, false, time, GetCurrentTimestamp())
	}
}

func getOrInitFromMap(instrument string) *instrumentOrder {
	item, present := instrumentMap[instrument]
	if present {
		return item
	}
	var res = &instrumentOrder{make(chan order)}
	instrumentMap[instrument] = res
	go instrumentHandler(res.dataStream)
	return res
}

func (e *Engine) accept(ctx context.Context, conn net.Conn) {
	go func() {
		<-ctx.Done()
		conn.Close()
	}()
	go handleConn(conn)
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		in, err := readInput(conn)
		if err != nil {
			if err != io.EOF {
				_, _ = fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			}
			return
		}
		timeNow := GetCurrentTimestamp()

		switch in.orderType {
		case inputCancel:
			for _, instrOrder := range instrumentMap {
				var sendChan (chan<- order) = instrOrder.dataStream
				var data = order{in, timeNow}
				sendChan <- data
			}
		default:
			var instrOrder = getOrInitFromMap(in.instrument) // struct with channel for an instrument
			var sendChan (chan<- order) = instrOrder.dataStream
			var data = order{in, timeNow}
			sendChan <- data
		}
	}
}

func GetCurrentTimestamp() int64 {
	return time.Now().UnixMicro()
}
