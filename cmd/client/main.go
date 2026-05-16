package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatal("could not connect to RabbitMQ: ", err)
	}
	defer conn.Close()
	fmt.Println("Peril game client connected to RabbitMQ")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("could not create channel: ", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal("could not get username: ", err)
	}
	gs := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		conn, 
		routing.ExchangePerilTopic, 
		routing.ArmyMovesPrefix+"."+gs.GetUsername(), 
		routing.ArmyMovesPrefix+".*", 
		pubsub.SimpleQueueTransient, 
		handlerMove(gs, ch),
	)
	if err != nil {
		log.Fatal("could not subscribe to army moves: ", err)
	}
	err = pubsub.SubscribeJSON(
		conn, 
		routing.ExchangePerilDirect, 
		routing.PauseKey+"."+gs.GetUsername(), 
		routing.PauseKey, 
		pubsub.SimpleQueueDurable, 
		handlerPause(gs),
	)
	if err != nil {
		log.Fatal("could not subscribe to pause: ", err)
	}
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war",
		routing.WarRecognitionsPrefix+".*",
		pubsub.SimpleQueueDurable,
		handlerWar(gs, ch),
	)
	if err != nil {
		log.Fatal("could not subscribe war outcome: ", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			err = gs.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			mv, err := gs.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = pubsub.PublishJSON(
				ch, 
				routing.ExchangePerilTopic, 
				routing.ArmyMovesPrefix+"."+mv.Player.Username,
				mv,
			)
			if err != nil {
				log.Printf("could not publish move: %v", err)
				continue
			}
			fmt.Printf("Moved %v units to %s\n", len(mv.Units), mv.ToLocation)
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unknown command try again")
		}
	}
}