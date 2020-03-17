package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/sarweshsuman/redis-cluster-go-coordinator/config"
	"github.com/sarweshsuman/redis-cluster-go-coordinator/utils"
)

func main() {
	session, err := utils.CreateRedisSession()
	if err != nil {
		panic(err)
	}

	var flagIsClusterCreated = false
	var redisClusterSeedNodes = []string{}

	val, err := session.Connection.Exists(config.FlagIsClusterCreated).Result()
	if err != nil {
		panic(err)
	}

	if val == 1 {
		val, err := session.Connection.Get(config.FlagIsClusterCreated).Result()
		if err != nil {
			panic(err)
		}
		if val == "true" {
			flagIsClusterCreated = true
		} else {
			flagIsClusterCreated = false
		}

	} else {
		flagIsClusterCreated = false
	}

	if flagIsClusterCreated == false {

		var val int64
		val, err = session.Connection.LLen(config.ClusterRegistrationQueue).Result()
		if err != nil {
			panic(err)
		}

		for val < config.NumOfSeedNodes {
			time.Sleep(5 * time.Second)

			val, err = session.Connection.LLen(config.ClusterRegistrationQueue).Result()
			if err != nil {
				panic(err)
			}
		}

		redisClusterCreateCmd := []string{}

		redisClusterCreateCmd = append(redisClusterCreateCmd, "--cluster")
		redisClusterCreateCmd = append(redisClusterCreateCmd, "create")

		for i := int64(0); i < config.NumOfSeedNodes; i++ {
			val, err := session.Connection.RPop(config.ClusterRegistrationQueue).Result()
			if err != nil {
				panic(err)
			}
			redisClusterCreateCmd = append(redisClusterCreateCmd, val)
			session.Connection.LPush(config.RegisteredNodesQueue, val)
			redisClusterSeedNodes = append(redisClusterSeedNodes, val)
		}

		redisClusterCreateCmd = append(redisClusterCreateCmd, "--cluster-replicas")
		redisClusterCreateCmd = append(redisClusterCreateCmd, config.ClusterReplicas)
		redisClusterCreateCmd = append(redisClusterCreateCmd, "--cluster-yes")

		//joinedRedisClusterNode := strings.TrimLeft(strings.Join(redisClusterNodes, " "), " ")
		cmd := exec.Command("redis-cli", redisClusterCreateCmd...)

		var out bytes.Buffer

		cmd.Stdout = &out
		cmd.Stderr = &out

		err := cmd.Run()
		if err != nil {
			fmt.Printf("Failed to create cluster.\n%v\n", out.String())
			panic(err)
		}
		fmt.Printf("Created cluster successful.\n%v\n", out.String())

		session.Connection.Set(config.FlagIsClusterCreated, "true", 0)

	}
	// Cluster is created, lets monitor and add nodes if needed

	if len(redisClusterSeedNodes) == 0 {
		redisClusterSeedNodes, err = session.Connection.LRange(config.RegisteredNodesQueue, 0, -1).Result()
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("Starting monitoring for additional nodes")
	for true {
		var val int64
		val, err = session.Connection.LLen(config.ClusterRegistrationQueue).Result()
		if err != nil {
			panic(err)
		}

		if val > 0 {
			node, err := session.Connection.RPop(config.ClusterRegistrationQueue).Result()
			if err != nil {
				panic(err)
			}

			fmt.Printf("Found a node to add %v\n", node)
			redisClusterAddCmd := []string{}

			redisClusterAddCmd = append(redisClusterAddCmd, "--cluster")
			redisClusterAddCmd = append(redisClusterAddCmd, "add-node")
			redisClusterAddCmd = append(redisClusterAddCmd, node)
			fmt.Println(redisClusterSeedNodes)
			for i := 0; i < len(redisClusterSeedNodes); i++ {
				redisClusterAddCmd = append(redisClusterAddCmd, redisClusterSeedNodes[i])
				cmd := exec.Command("redis-cli", redisClusterAddCmd...)

				fmt.Println(cmd)

				var out bytes.Buffer

				cmd.Stdout = &out
				cmd.Stderr = &out

				err := cmd.Run()
				if err != nil {
					fmt.Printf("Failed to add %v to cluster.\n%v\n", node, out.String())
					redisClusterAddCmd = redisClusterAddCmd[0 : len(redisClusterAddCmd)-1]

					if i == len(redisClusterSeedNodes)-1 {
						fmt.Printf("Failed to register node %v with any cluster nodes.", node)
						panic("Failed to register node")
					}
					continue
				}
				fmt.Printf("Added %v to cluster successfully.\n%v\n", node, out.String())
				session.Connection.LPush(config.PendingClusterNodes, node)
				break
			}

		}

		time.Sleep(10 * time.Second)

		val, err = session.Connection.LLen(config.PendingClusterNodes).Result()
		if err != nil {
			panic(err)
		}

		if val > 0 {
			node, err := session.Connection.RPop(config.PendingClusterNodes).Result()
			if err != nil {
				panic(err)
			}
			fmt.Printf("Found a cluster node to process as master or slave %v\n", node)

			fmt.Println("Checking if any existing master nodes memory is above limit")
			maxMemory := 0
			maxMemoryNodeID := -1
			// Get Master with Highest Memory
			for idx := range redisClusterSeedNodes {
				connection, err := utils.CreateRedisSessionTo(redisClusterSeedNodes[idx])
				if err != nil {
					panic(err)
				}
				isMaster, err := connection.IsMaster()
				if err != nil {
					panic(err)
				}
				if isMaster == true {
					fmt.Printf("Checking master %v\n", redisClusterSeedNodes[idx])

					memoryInStr, err := connection.GetNodeMemoryUsage()
					if err != nil {
						panic(err)
					}
					memoryInInt, err := strconv.Atoi(*memoryInStr)
					if err != nil {
						panic(err)
					}
					if memoryInInt > maxMemory {
						maxMemory = memoryInInt
						maxMemoryNodeID = idx
					}
				}
			}

			fmt.Printf("Node with Max Memory Usage Found %v Max Memory %v \n", redisClusterSeedNodes[maxMemoryNodeID], maxMemory)

			if maxMemoryNodeID != -1 && float64(maxMemory) > config.MemoryThresholdForRedis {
				// Take Slot from Master maxMemoryNodeID

				fmt.Println("Max Memory is above threshold, will proceed to reshard this master")

				redisClusterReshardCmd := []string{}

				redisClusterReshardCmd = append(redisClusterReshardCmd, "--cluster")
				redisClusterReshardCmd = append(redisClusterReshardCmd, "reshard")
				redisClusterReshardCmd = append(redisClusterReshardCmd, redisClusterSeedNodes[maxMemoryNodeID])
				redisClusterReshardCmd = append(redisClusterReshardCmd, "--cluster-from")
				nodeID, err := utils.GetNodeID(redisClusterSeedNodes[maxMemoryNodeID])
				if err != nil {
					panic(err)
				}

				redisClusterReshardCmd = append(redisClusterReshardCmd, *nodeID)
				redisClusterReshardCmd = append(redisClusterReshardCmd, "--cluster-to")

				nodeID, err = utils.GetNodeID(node)
				if err != nil {
					panic(err)
				}
				redisClusterReshardCmd = append(redisClusterReshardCmd, *nodeID)
				redisClusterReshardCmd = append(redisClusterReshardCmd, "--cluster-slots")

				slots, err := utils.GetNodeTotalSlots(redisClusterSeedNodes[maxMemoryNodeID])
				fmt.Println()
				fmt.Println(*slots)
				fmt.Println()
				if err != nil {
					panic(err)
				}

				redisClusterReshardCmd = append(redisClusterReshardCmd, strconv.Itoa((*slots)/2))
				redisClusterReshardCmd = append(redisClusterReshardCmd, "--cluster-yes")

				cmd := exec.Command("redis-cli", redisClusterReshardCmd...)

				fmt.Println(cmd)

				var out bytes.Buffer

				cmd.Stdout = &out
				cmd.Stderr = &out

				err = cmd.Run()
				if err != nil {
					fmt.Printf("Failed to transfer slot %v to cluster.\n%v\n", node, out.String())
					panic(err)
				}
				fmt.Printf("Successfully transfered slots to node %v.\n%v\n", node, out.String())

				session.Connection.LPush(config.RegisteredNodesQueue, node)

			} else {
				// Will Look If some slave is down and add it there

				fmt.Printf("Will attempt to add the node %v as a slave of existing master if a slave is missing from any master.\n", node)

				clusterClientNode, err := utils.CreateRedisSessionTo(redisClusterSeedNodes[0])
				if err != nil {
					panic(err)
				}

				// Checking Slaves
				masterNodes, err := clusterClientNode.GetClusterMasterNodes()
				if err != nil {
					panic(err)
				}

				fmt.Printf("List of Master Nodes %v\n", masterNodes)

				for idx := range masterNodes {

					_, ok := masterNodes[idx]["slots"]
					if ok == false {
						continue
					}

					slaves, err := clusterClientNode.GetSlavesForAMaster(masterNodes[idx]["nodeid"])
					if err != nil {
						panic(err)
					}

					fmt.Printf("List of Slaves for Master NodeID \n%v : %v\n", masterNodes[idx], slaves)
					if len(slaves) < config.MinNoOfSlaves {

						// redis-cli -p 7007 cluster replicate 5111d7b0ef9124d5682ccdbdeeedcb4a3960b5a3
						// Configure this Node as Slave
						// break, continue
						redisClusterAddSlaveCmd := []string{}

						arr := strings.Split(node, ":")

						redisClusterAddSlaveCmd = append(redisClusterAddSlaveCmd, "-h")
						redisClusterAddSlaveCmd = append(redisClusterAddSlaveCmd, arr[0])
						redisClusterAddSlaveCmd = append(redisClusterAddSlaveCmd, "-p")
						redisClusterAddSlaveCmd = append(redisClusterAddSlaveCmd, arr[1])
						redisClusterAddSlaveCmd = append(redisClusterAddSlaveCmd, "cluster")
						redisClusterAddSlaveCmd = append(redisClusterAddSlaveCmd, "replicate")
						redisClusterAddSlaveCmd = append(redisClusterAddSlaveCmd, masterNodes[idx]["nodeid"])

						cmd := exec.Command("redis-cli", redisClusterAddSlaveCmd...)

						fmt.Println(cmd)

						var out bytes.Buffer

						cmd.Stdout = &out
						cmd.Stderr = &out

						err = cmd.Run()
						if err != nil {
							fmt.Printf("Failed to add node %v as slave.\n%v\n", node, out.String())
							panic(err)
						}
						fmt.Printf("Successfully added node %v as slave.\n%v\n", node, out.String())

						session.Connection.LPush(config.RegisteredNodesQueue, node)

						break

					}

				}
			}

		}

		time.Sleep(5 * time.Second)
	}

}
