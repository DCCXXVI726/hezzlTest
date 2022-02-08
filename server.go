package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	userpc "github.com/DCCXXVI726/hezzlTest/user"
	"github.com/go-redis/redis"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"os"
	"os/signal"
	"time"
)

var (
	infos      []userItem
	db         *sql.DB
	err        error
	cache      *redis.Client
	clickHouse *sql.DB
)

const defaultPort = "7262"

type server struct {
	userpc.UserServiceServer
}
type userItem struct {
	ID  int
	Pid string
}

func (*server) AddUser(ctx context.Context, req *userpc.AddUserRequest) (*userpc.AddUserResponse, error) {
	fmt.Println("Add User")
	user := req.GetUser()

	insertData := `insert into users(Pid) values($1) returning id`
	var lastInsertId int64
	err := db.QueryRow(insertData, user.GetPid()).Scan(&lastInsertId)
	if err != nil {
		err = fmt.Errorf("can't insert data: %v", err)
		panic(err)
	}
	tx, err := clickHouse.Begin()
	if err != nil {
		panic(err)
	}
	stmt, err := tx.Prepare(`
		INSERT INTO log (
			data_time,
			levelmsg,
			message
		) VALUES (
			?, ?, ?
		)`)
	if err != nil {
		panic(err)
	}
	message := fmt.Sprintf("add user with pid: %s, id: %v", user.GetPid(), lastInsertId)
	_, err = stmt.Exec(time.Now(), "info", message)
	if err = tx.Commit(); err != nil {
		panic(err)
	}
	if err != nil {
		panic(err)
	}
	return &userpc.AddUserResponse{
		User: &userpc.User{
			Id:  lastInsertId,
			Pid: user.GetPid(),
		},
	}, nil
}

func (*server) DeleteUser(ctx context.Context, req *userpc.DeleteUserRequest) (*userpc.DeleteUserResponse, error) {
	fmt.Println("Delete User")

	pid := req.GetPid()

	deleteData := `DELETE from users where Pid = $1`
	_, err := db.Exec(deleteData, pid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("don't find object: %v", err))
	}

	return &userpc.DeleteUserResponse{Pid: req.GetPid()}, nil
}

func (*server) ListUser(_ *userpc.ListUserRequest, stream userpc.UserService_ListUserServer) error {
	var list []userpc.User
	listBytes, err := cache.Get("list").Bytes()
	if err != nil {
		rows, err := db.Query(`Select ID, Pid from users`)
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		for rows.Next() {
			var user userpc.User
			err = rows.Scan(&user.Id, &user.Pid)
			if err != nil {
				panic(err)
			}
			list = append(list, user)

		}
		listBytes, err = json.Marshal(list)
		if err != nil {
			panic(err)
		}
		err = cache.Set("list", listBytes, 60*time.Second).Err()
		if err != nil {
			panic(err)
		}
	} else {
		err = json.Unmarshal(listBytes, &list)
		if err != nil {
			panic(err)
		}
	}
	for _, user := range list {
		stream.Send(&userpc.ListUserResponse{User: &user})
	}

	return nil
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	// connect to clickhouse
	clickHouse, err = sql.Open("clickhouse", "http://127.0.0.1:9000/default")
	if err != nil {
		log.Fatal(err)
	}

	_, err = clickHouse.Exec(`
		CREATE TABLE IF NOT EXISTS log (
			data_time	DateTime,
			levelmsg	String,
			message		String
		) engine=Memory
	`)

	if err != nil {
		log.Fatal(err)
	}

	if err := clickHouse.Ping(); err != nil {
		log.Fatal(err)
	}
	//connect to redis
	cache = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	pong, err := cache.Ping().Result()
	fmt.Println(pong, err)

	//connect to postgres
	postgres_usl := "postgres://vxlcnqtteqxdln:51cb1f0beb87b5076accaa967e28c91b874b712cdfdc7251b0c60e944d97def4@ec2-34-205-46-149.compute-1.amazonaws.com:5432/d22f6uegt629m2"

	db, err = sql.Open("postgres", postgres_usl)
	if err != nil {
		panic(err)
	}
	lis, err := net.Listen("tcp", "0.0.0.0:"+defaultPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	//starting server
	var opts []grpc.ServerOption
	s := grpc.NewServer(opts...)

	userpc.RegisterUserServiceServer(s, &server{})

	reflection.Register(s)

	go func() {
		fmt.Println("Starting server")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)

	// Block until a signal is received
	<-ch
	fmt.Println("Stopping the server")
	s.Stop()
	fmt.Println("End of Program")
}
