package utils

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v7"
	"github.com/sarweshsuman/redis-cluster-go-coordinator/config"
)

type RedisSession struct {
	Connection        *redis.Client
	ClusterConnection *redis.ClusterClient
}

func CreateRedisSession() (*RedisSession, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr,
		Password: config.RedisPassword,
		DB:       config.RedisDb,
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}
	//fmt.Printf("Connection successful to redis ping:%v\n", pong)

	session := &RedisSession{Connection: client}
	return session, nil
}

func CreateRedisSessionTo(node string) (*RedisSession, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     node,
		Password: config.RedisPassword,
		DB:       config.RedisDb,
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}
	//fmt.Printf("Connection successful to redis ping:%v\n", pong)

	session := &RedisSession{Connection: client}
	return session, nil
}

func CreateClusterRedisSession(clusterNodes []string) (*RedisSession, error) {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: clusterNodes,
	})

	pong, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}
	fmt.Printf("Connection successful to redis ping:%v\n", pong)

	session := &RedisSession{ClusterConnection: client}
	return session, nil
}

func GetClusterMasterNodes(nodeIP string) ([]map[string]string, error) {

	session, err := CreateRedisSessionTo(nodeIP)
	if err != nil {
		return nil, err
	}

	nodesInStr, err := session.Connection.ClusterNodes().Result()
	if err != nil {
		return nil, err
	}

	nodes := strings.Split(nodesInStr, "\n")
	var masterNode = []map[string]string{}

	for idx := 0; idx < len(nodes); idx++ {
		if strings.Contains(string(nodes[idx]), "master") == true {
			mapp := map[string]string{}

			arr := strings.Split(string(nodes[idx]), " ")
			ip := strings.Split(arr[1], "@")

			mapp["ip"] = ip[0]
			mapp["nodeid"] = arr[0]
			if len(arr) == 9 {
				upperBound, err := strconv.Atoi(strings.Split(arr[len(arr)-1], "-")[1])
				if err != nil {
					return nil, err
				}
				lowerBound, err := strconv.Atoi(strings.Split(arr[len(arr)-1], "-")[0])
				if err != nil {
					return nil, err
				}
				slots := upperBound - lowerBound
				mapp["slots"] = strconv.Itoa(slots)
			}
			masterNode = append(masterNode, mapp)
		}
	}
	return masterNode, nil
}

func GetSlavesForAMaster(nodeIP string, nodeID string) ([]string, error) {

	session, err := CreateRedisSessionTo(nodeIP)
	if err != nil {
		return nil, err
	}

	slaves, err := session.Connection.ClusterSlaves(nodeID).Result()
	if err != nil {
		return nil, err
	}
	return slaves, nil
}

func GetNodeMemoryUsage(nodeIP string) (*string, error) {

	session, err := CreateRedisSessionTo(nodeIP)
	if err != nil {
		return nil, err
	}

	re := regexp.MustCompile(`used_memory:([0-9]+)`)

	strVal, err := session.Connection.Info("memory").Result()
	if err != nil {
		return nil, err
	}

	strVal = strings.Split(string(re.Find([]byte(strVal))), ":")[1]

	return &strVal, nil
}

func (client *RedisSession) IsMaster() (bool, error) {

	re := regexp.MustCompile(`role:master`)

	bVal, err := client.Connection.Info("Replication").Result()
	if err != nil {
		return false, err
	}

	strVal := string(re.Find([]byte(bVal)))

	if strVal == "" {
		return false, nil
	} else {
		return true, nil
	}

}

func GetNodeID(node string) (*string, error) {

	session, err := CreateRedisSessionTo(node)
	if err != nil {
		return nil, err
	}

	nodesInStr, err := session.Connection.ClusterNodes().Result()
	if err != nil {
		return nil, err
	}

	nodes := strings.Split(nodesInStr, "\n")

	for idx := range nodes {
		if strings.Contains(string(nodes[idx]), node) {
			nodeID := strings.Split(string(nodes[idx]), " ")[0]
			return &nodeID, nil
		}
	}
	return nil, errors.New("Node ID not found")
}

func GetNodeTotalSlots(node string) (*int, error) {

	client, err := CreateRedisSessionTo(node)
	if err != nil {
		return nil, err
	}

	nodesInStr, err := client.Connection.ClusterNodes().Result()
	if err != nil {
		return nil, err
	}
	nodes := strings.Split(nodesInStr, "\n")

	for idx := range nodes {
		if strings.Contains(string(nodes[idx]), node) {
			arr := strings.Split(string(nodes[idx]), " ")
			fmt.Println()
			fmt.Println(arr)
			fmt.Println()
			upperBound, err := strconv.Atoi(strings.Split(arr[len(arr)-1], "-")[1])
			if err != nil {
				return nil, err
			}
			lowerBound, err := strconv.Atoi(strings.Split(arr[len(arr)-1], "-")[0])
			if err != nil {
				return nil, err
			}
			slots := upperBound - lowerBound
			return &slots, nil
		}
	}
	return nil, errors.New("Node ID not found")
}
