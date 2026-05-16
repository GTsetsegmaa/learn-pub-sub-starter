package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)



func handlerLog() func(routing.GameLog) pubsub.Acktype {
	return func(gamelog routing.GameLog) pubsub.Acktype{
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gamelog)
		if err != nil {
			fmt.Printf("error writing to log: %v\n", err)
			return pubsub.NackRequeue
		} else {
			return pubsub.Ack
		}
	}
}