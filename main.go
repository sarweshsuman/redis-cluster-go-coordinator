package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/sarweshsuman/redis-cluster-go-coordinator/config"
	"github.com/sarweshsuman/redis-cluster-go-coordinator/utils"
)

func main() {
	fmt.Println("Connecting to local redis.")
	session, err := utils.CreateRedisSession()
	if err != nil {
		panic(err)
	}
	fmt.Println("Connected.")

	var flagIsClusterCreated = false
	var redisClusterSeedNodes = []string{}

	fmt.Println("Checking if cluster is already created or not?")
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

		fmt.Println("Cluster is not created, will wait for all the seed nodes to register request.")
		var val int64

		val, err = session.Connection.LLen(config.ClusterRegistrationQueue).Result()
		if err != nil {
			fmt.Printf("Error %v in retrieving length of ClusterRegistrationQueue\n", err)
			val = 0
		}

		// Wait for all the nodes to register request.
		for val < config.NumOfSeedNodes {
			time.Sleep(5 * time.Second)

			val, err = session.Connection.LLen(config.ClusterRegistrationQueue).Result()
			if err != nil {
				fmt.Printf("Error %v in retrieving length of ClusterRegistrationQueue\n", err)
				val = 0
				continue
			}
		}

		fmt.Println("All seed nodes have registed request, Moving ahead with cluster creation.")

		redisClusterCreateCmd := []string{}
		redisClusterCreateCmd = append(redisClusterCreateCmd, "--cluster")
		redisClusterCreateCmd = append(redisClusterCreateCmd, "create")

		for i := int64(0); i < config.NumOfSeedNodes; i++ {
			val, err := session.Connection.RPop(config.ClusterRegistrationQueue).Result()
			if err != nil {
				fmt.Printf("Error %v in retrieving seed nodes from ClusterRegistrationQueue, re-attempt in few seconds\n", err)
				i--
				time.Sleep(10 * time.Second)
				continue
			}
			redisClusterCreateCmd = append(redisClusterCreateCmd, val)
			redisClusterSeedNodes = append(redisClusterSeedNodes, val)
		}

		redisClusterCreateCmd = append(redisClusterCreateCmd, "--cluster-replicas")
		redisClusterCreateCmd = append(redisClusterCreateCmd, config.ClusterReplicas)
		redisClusterCreateCmd = append(redisClusterCreateCmd, "--cluster-yes")

		for {
			err = utils.RedisCliCmd(redisClusterCreateCmd)
			if err != nil {
				fmt.Printf("Cluster creation failed due to %v, re-attempt in few seconds\n", err)
				time.Sleep(10 * time.Second)
				continue
			}
			break
		}

		for idx := range redisClusterSeedNodes {
			session.Connection.LPush(config.RegisteredNodesQueue, redisClusterSeedNodes[idx])
		}
		session.Connection.Set(config.FlagIsClusterCreated, "true", 0)
		fmt.Println("Cluster creation complete.")

	} else {
		fmt.Println("Cluster is already created.")
	}

	// Cluster is created, lets monitor and add nodes if needed
	if len(redisClusterSeedNodes) == 0 {
		for {
			redisClusterSeedNodes, err = session.Connection.LRange(config.RegisteredNodesQueue, 0, -1).Result()
			if err != nil {
				fmt.Printf("Error %v in retrieving seed nodes, re-attempt in few seconds\n", err)
				time.Sleep(10 * time.Second)
				continue
			}
			break
		}
	}

	fmt.Printf("Seed Nodes registered are %v\n", redisClusterSeedNodes)
	fmt.Println("Starting monitoring for additional nodes.")

MAINLOOP:
	for true {

		var queueLength int64
		queueLength, err = session.Connection.LLen(config.ClusterRegistrationQueue).Result()
		if err != nil {
			fmt.Printf("Error %v in retrieving ClusterRegistrationQueue queue length\n", err)
			time.Sleep(10 * time.Second)
			continue
		}

		if queueLength > 0 {

			newNode, err := session.Connection.RPop(config.ClusterRegistrationQueue).Result()
			if err != nil {
				fmt.Printf("Error %v in retrieving node from ClusterRegistrationQueue\n", err)
				time.Sleep(10 * time.Second)
				continue
			}

			fmt.Printf("Found a node to process %v\n", newNode)

			redisClusterAddCmd := []string{}
			redisClusterAddCmd = append(redisClusterAddCmd, "--cluster")
			redisClusterAddCmd = append(redisClusterAddCmd, "add-node")
			redisClusterAddCmd = append(redisClusterAddCmd, newNode)

			for i := 0; i < len(redisClusterSeedNodes); i++ {
				redisClusterAddCmd = append(redisClusterAddCmd, redisClusterSeedNodes[i])

				fmt.Printf("Attempting to add node with seed node %v\n", redisClusterSeedNodes[i])
				err = utils.RedisCliCmd(redisClusterAddCmd)
				if err != nil {
					fmt.Printf("Failed to add %v to cluster.\n", newNode)
					fmt.Printf("Error: %v\n", err)

					redisClusterAddCmd = redisClusterAddCmd[0 : len(redisClusterAddCmd)-1]

					if i == len(redisClusterSeedNodes)-1 {
						fmt.Printf("Failed to register node %v with any cluster nodes.re-attempt in few seconds\n", newNode)
						session.Connection.LPush(config.ClusterRegistrationQueue, newNode)
						time.Sleep(10 * time.Second)
						continue MAINLOOP
					}
					continue
				}

				fmt.Printf("Added %v to cluster successfully. Added to the PendingClusterNodes Queue.\n", newNode)
				session.Connection.LPush(config.PendingClusterNodes, newNode)
				break
			}

		} // End of queueLength > 0 if statment

		// Will wait 10 seconds before start to either process that node as master with some slots or slave to some existing master.
		time.Sleep(10 * time.Second)

		queueLength, err = session.Connection.LLen(config.PendingClusterNodes).Result()
		if err != nil {
			fmt.Printf("Error %v in retrieving PendingClusterNode queue length\n", err)
			time.Sleep(10 * time.Second)
			continue
		}

		if queueLength > 0 {

			newNode, err := session.Connection.RPop(config.PendingClusterNodes).Result()
			if err != nil {
				fmt.Printf("Error %v in retrieving node from PendingClusterNodes\n", err)
				continue
			}

			fmt.Printf("Found a cluster node to process as master or slave %v.\n", newNode)
			fmt.Printf("Attempt to add node %v as master and taking some slots from a master with most memory usage.\n", newNode)

			var masterNodes []map[string]string

			for idx := 0; idx < len(redisClusterSeedNodes); idx++ {
				masterNodes, err = utils.GetClusterMasterNodes(redisClusterSeedNodes[idx])
				if err != nil {
					fmt.Printf("Error %v while retrieving master nodes via node %v.\n", err, redisClusterSeedNodes[idx])
					if idx == len(redisClusterSeedNodes)-1 {
						fmt.Println("All attempts exhausted, will re-attempt in few seconds")
						time.Sleep(10 * time.Second)
						continue MAINLOOP
					}
					continue
				}
				fmt.Printf("Got the list of master nodes %v.\n", masterNodes)
				break
			}

			maxMemory := 0
			maxMemoryNodeID := -1
			// Get Master with Highest Memory
			for idx := 0; idx < len(masterNodes); idx++ {
				memoryInStr, err := utils.GetNodeMemoryUsage(masterNodes[idx]["ip"])
				if err != nil {
					fmt.Printf("Error in retrieving memory details for master node %v\n", masterNodes[idx]["ip"])
					continue
				}
				if masterNodes[idx]["ip"] == newNode {
					continue
				}

				memoryInInt, err := strconv.Atoi(*memoryInStr)
				if err != nil {
					fmt.Printf("Error in retrieving memory details for master node %v\n", masterNodes[idx]["ip"])
					continue
				}

				if memoryInInt > maxMemory {
					maxMemory = memoryInInt
					maxMemoryNodeID = idx
				}

			}
			fmt.Printf("Node with Max Memory Usage: %v Memory Usage: %v \n", masterNodes[maxMemoryNodeID]["ip"], maxMemory)

			fmt.Println("Checking if a master node exists whose memory usage is more than threshold")
			if maxMemoryNodeID != -1 && float64(maxMemory) > config.MemoryThresholdForRedis {
				// Take Slot from Master maxMemoryNodeID

				fmt.Printf("Master node %v has memory usage more than threshold, attempt to take some slots from this master memory.\n", masterNodes[maxMemoryNodeID]["ip"])

				redisClusterReshardCmd := []string{}
				redisClusterReshardCmd = append(redisClusterReshardCmd, "--cluster")
				redisClusterReshardCmd = append(redisClusterReshardCmd, "reshard")
				redisClusterReshardCmd = append(redisClusterReshardCmd, masterNodes[maxMemoryNodeID]["ip"])
				redisClusterReshardCmd = append(redisClusterReshardCmd, "--cluster-from")
				redisClusterReshardCmd = append(redisClusterReshardCmd, masterNodes[maxMemoryNodeID]["nodeid"])
				redisClusterReshardCmd = append(redisClusterReshardCmd, "--cluster-to")

				nodeID, err := utils.GetNodeID(newNode)
				if err != nil {
					fmt.Printf("Error %v while retrieving new node %v nodeid\n", err, newNode)
					session.Connection.LPush(config.PendingClusterNodes, newNode)
					time.Sleep(10 * time.Second)
					continue MAINLOOP
				}
				redisClusterReshardCmd = append(redisClusterReshardCmd, *nodeID)
				redisClusterReshardCmd = append(redisClusterReshardCmd, "--cluster-slots")

				slots, ok := masterNodes[maxMemoryNodeID]["slots"]
				if ok == false {
					fmt.Printf("Unable to retireve slots for master node %v\n", masterNodes[maxMemoryNodeID]["ip"])
					session.Connection.LPush(config.PendingClusterNodes, newNode)
					time.Sleep(10 * time.Second)
					continue MAINLOOP
				}

				slotsInInt, err := strconv.Atoi(slots)
				if err != nil {
					fmt.Printf("Unable to convert slots %v of master node %v to string\n", slots, masterNodes[maxMemoryNodeID]["ip"])
					session.Connection.LPush(config.PendingClusterNodes, newNode)
					time.Sleep(10 * time.Second)
					continue MAINLOOP
				}

				redisClusterReshardCmd = append(redisClusterReshardCmd, strconv.Itoa((slotsInInt)/2))
				redisClusterReshardCmd = append(redisClusterReshardCmd, "--cluster-yes")

				err = utils.RedisCliCmd(redisClusterReshardCmd)
				if err != nil {
					fmt.Printf("Error %v in resharding slots from %v to %v\n", err, masterNodes[maxMemoryNodeID]["ip"], newNode)
					session.Connection.LPush(config.PendingClusterNodes, newNode)
					time.Sleep(10 * time.Second)
					continue MAINLOOP
				}

				session.Connection.LPush(config.RegisteredNodesQueue, newNode)
				fmt.Printf("Processing of new node %v complete.\n", newNode)

			} else {

				fmt.Println("No master node exists with high memory usage. Therefore,")
				fmt.Printf("Attempt to add node %v as slave of existing master if a slave is missing from any master.\n", newNode)

				flagNodeProcessed := false

				for idx := range masterNodes {

					if newNode == masterNodes[idx]["ip"] {
						continue
					}

					slaves, err := utils.GetSlavesForAMaster(masterNodes[idx]["ip"], masterNodes[idx]["nodeid"])
					if err != nil {
						fmt.Printf("Error %v in retrieving list of slaves for master node %v\n", err, masterNodes[idx]["ip"])
						continue
					}

					fmt.Printf("Slaves for master node %v : %v\n", masterNodes[idx]["ip"], slaves)
					if len(slaves) < config.MinNoOfSlaves {

						fmt.Printf("Num of Slaves less than min threshold of %v\n", config.MinNoOfSlaves)

						redisClusterAddSlaveCmd := []string{}
						arr := strings.Split(newNode, ":")
						redisClusterAddSlaveCmd = append(redisClusterAddSlaveCmd, "-h")
						redisClusterAddSlaveCmd = append(redisClusterAddSlaveCmd, arr[0])
						redisClusterAddSlaveCmd = append(redisClusterAddSlaveCmd, "-p")
						redisClusterAddSlaveCmd = append(redisClusterAddSlaveCmd, arr[1])
						redisClusterAddSlaveCmd = append(redisClusterAddSlaveCmd, "cluster")
						redisClusterAddSlaveCmd = append(redisClusterAddSlaveCmd, "replicate")
						redisClusterAddSlaveCmd = append(redisClusterAddSlaveCmd, masterNodes[idx]["nodeid"])

						err = utils.RedisCliCmd(redisClusterAddSlaveCmd)
						if err != nil {
							fmt.Printf("Error in registering new node %v as replica of master node %v\n", newNode, masterNodes[idx]["ip"])
							continue
						}

						session.Connection.LPush(config.RegisteredNodesQueue, newNode)
						flagNodeProcessed = true

						fmt.Printf("Successfully configured node %v to be replica of node %v\n", newNode, masterNodes[idx]["ip"])
						break

					} // end of if len(slaves) < config.MinNoOfSlaves

				} // end of for line num 268

				if flagNodeProcessed == false {
					fmt.Printf("Unable to process new node %v as neither memory or slave condition satisfied\n", newNode)
					session.Connection.LPush(config.PendingClusterNodes, newNode)
				}

			} // end of else for line num 278

		} // end of if queueLength > 0

		time.Sleep(5 * time.Second)
	} // end of for true

}
