package config

var (
	// RedisAddr default value of redis connection
	RedisAddr = "127.0.0.1:6379"
	// RedisPassword default value of redis connection
	RedisPassword = ""
	// RedisDb default value of redis connection
	RedisDb = 0
)

var (
	// ClusterRegistrationQueue is list where redis cluster pods will register it self.
	ClusterRegistrationQueue = "REDIS_CLUSTER_REGISTRATION_QUEUE"
	// RegisteredNodesQueue ...
	RegisteredNodesQueue = "REDIS_CLUSTER_NODES"
	// PendingClusterNodes ...
	PendingClusterNodes = "PENDING_CLUSTER_NODES"
	// NumOfSeedNodes is initial set of redis cluster node to create cluster
	NumOfSeedNodes int64 = 6
	// FlagIsClusterCreated ...
	FlagIsClusterCreated = "IS_CLUSTER_CREATED"
	// ClusterReplicas ...
	ClusterReplicas = "1"
	// MinNoOfSlaves ...
	MinNoOfSlaves = 1
	// MemoryThresholdForRedis ...
	MemoryThresholdForRedis = 6741456.40
)
