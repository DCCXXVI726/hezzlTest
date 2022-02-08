package main

import (
	"context"
	"fmt"
	userpc "github.com/DCCXXVI726/hezzlTest/user"
	"io"
	"log"
	"os"

	"google.golang.org/grpc"
)

const defaultPort = "7262"

func main() {
	fmt.Println("Creating Client")

	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}
	opts := grpc.WithInsecure()

	cc, err := grpc.Dial("localhost:"+port, opts)
	if err != nil {
		log.Fatalf("could not protect: %v", err)
	}
	defer cc.Close()

	c := userpc.NewUserServiceClient(cc)

	fmt.Println("Add the user")
	user := &userpc.User{
		Pid: "alexey",
	}
	addUserRes, err := c.AddUser(context.Background(), &userpc.AddUserRequest{User: user})
	if err != nil {
		log.Fatalf("Unexpected error: %v", err)
	}
	fmt.Println("User has been added: %v", addUserRes)

	//второй пользователь
	addUserRes, err = c.AddUser(context.Background(), &userpc.AddUserRequest{User: user})
	if err != nil {
		log.Fatalf("Unexpected error: %v", err)
	}
	fmt.Println("User has been added: %v", addUserRes)

	stream, err := c.ListUser(context.Background(), &userpc.ListUserRequest{})
	if err != nil {
		log.Fatalf("error while calling ListUser RPC: %v", err)
	}
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Something happend: %v", err)
		}
		fmt.Println(res.GetUser())
	}
	deleteRes, err := c.DeleteUser(context.Background(), &userpc.DeleteUserRequest{
		Pid: "alexey",
	})
	if err != nil {
		fmt.Printf("Error happened while deleting: %v \n", err)
	}
	fmt.Printf("Pokemon was deleted: %v \n", deleteRes)

}
