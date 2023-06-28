package main

import (
	"encoding/json"
	"fmt"
	"sync"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/fablopes/InvestIQ/go/internal/infra/kafka"
	"github.com/fablopes/InvestIQ/go/internal/market/dto"
	"github.com/fablopes/InvestIQ/go/internal/market/entity"
	"github.com/fablopes/InvestIQ/go/internal/market/transformer"
)

func main() {
	ordersIn := make(chan *entity.Order)
	ordersOut := make(chan *entity.Order)
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	kafkaMsgChan := make(chan *ckafka.Message)
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers": "host.docker.internal:9094",
		"group.id":          "myGroup",
		"auto.offset.reset": "latest",
	}
	producer := kafka.NewKafkaProducer(configMap)
	kafka := kafka.NewConsumer(configMap, []string{"input"})

	go kafka.Consume(kafkaMsgChan)

	book := entity.NewBook(ordersIn, ordersOut, wg)
	go book.Trade()

	go func() {
		for msg := range kafkaMsgChan {
			wg.Add(1)
			fmt.Println(string(msg.Value))
			tradeInput := dto.TradeInput{}
			err := json.Unmarshal(msg.Value, &tradeInput)
			if err != nil {
				panic(err)
			}
			order := transformer.TransformerInput(tradeInput)
			ordersIn <- order
		}
	}()

	for res := range ordersOut {
		output := transformer.TransformerOutput(res)
		outupJson, err := json.MarshalIndent(output, "", " ")
		fmt.Println(string(outupJson))
		if err != nil {
			fmt.Println(err)
		}
		producer.Publish(outupJson, []byte("orders"), "output")
	}
}
