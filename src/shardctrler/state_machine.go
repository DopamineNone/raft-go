package shardctrler

import (
	"maps"
	"sort"
)

type ControllerStateMachine struct {
	// field must be upper case for serialization
	config []Config
}

func NewControllerStateMachine() *ControllerStateMachine {
	csm := &ControllerStateMachine{
		config: make([]Config, 1),
	}
	csm.config[0] = DefaultConfig()
	return csm
}

func (csm *ControllerStateMachine) Query(num int) (Config, Err) {
	if num < 0 || num >= len(csm.config) {
		return csm.config[len(csm.config)-1], OK
	}
	return csm.config[num], OK
}

func (csm *ControllerStateMachine) Join(groups map[int][]string) Err {
	num := len(csm.config)
	lastConfig := csm.config[num-1]

	// build new config
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: make(map[int][]string, len(lastConfig.Groups)),
	}
	maps.Copy(newConfig.Groups, lastConfig.Groups)

	// let new group join the last config
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	gidToShard := make(map[int][]int)
	// initialize gid map (necessary  for first config)
	for gid, _ := range newConfig.Groups {
		gidToShard[gid] = make([]int, 0)
	}
	for shard, gid := range newConfig.Shards {
		gidToShard[gid] = append(gidToShard[gid], shard)
	}

	// migrate shards
	for {
		gidWithMaxShard, gidWithMinShard := gidWithMaxShards(gidToShard), gidWithMinShards(gidToShard)
		if gidWithMaxShard != 0 && len(gidToShard[gidWithMinShard])+1 >= len(gidToShard[gidWithMaxShard]) {
			break
		}
		gidToShard[gidWithMinShard] = append(gidToShard[gidWithMinShard], gidToShard[gidWithMaxShard][0])
		gidToShard[gidWithMaxShard] = gidToShard[gidWithMaxShard][1:]
	}

	for gid, shards := range gidToShard {
		for _, shard := range shards {
			newConfig.Shards[shard] = gid
		}
	}

	csm.config = append(csm.config, newConfig)
	return OK
}

func (csm *ControllerStateMachine) Leave(gids []int) Err {
	num := len(csm.config)
	lastConfig := csm.config[num-1]

	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: make(map[int][]string, len(lastConfig.Groups)),
	}
	maps.Copy(newConfig.Groups, lastConfig.Groups)

	// gid -> shards
	gidToShard := make(map[int][]int)
	// initialize gid map (necessary  for groups with no shards)
	for gid, _ := range newConfig.Groups {
		gidToShard[gid] = make([]int, 0)
	}
	for shard, gid := range newConfig.Shards {
		gidToShard[gid] = append(gidToShard[gid], shard)
	}

	// remove corresponding gid
	var unassignedShards []int
	for _, gid := range gids {
		delete(newConfig.Groups, gid)
		unassignedShards = append(unassignedShards, gidToShard[gid]...)
		delete(gidToShard, gid)
	}

	// assign (p.s. once gitToShard is empty, newConfig.Shards also will be reset with 0)
	for _, shard := range unassignedShards {
		gidWithMinShard := gidWithMinShards(gidToShard)
		gidToShard[gidWithMinShard] = append(gidToShard[gidWithMinShard], shard)
		newConfig.Shards[shard] = gidWithMinShard
	}

	csm.config = append(csm.config, newConfig)
	return OK
}

func (csm *ControllerStateMachine) Move(shardId, gid int) Err {
	num := len(csm.config)
	lastConfig := csm.config[num-1]

	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: make(map[int][]string, len(lastConfig.Groups)),
	}
	maps.Copy(newConfig.Groups, lastConfig.Groups)

	newConfig.Shards[shardId] = gid
	csm.config = append(csm.config, newConfig)
	return OK
}

func gidWithMaxShards(gidToShars map[int][]int) int {
	if shard, ok := gidToShars[0]; ok && len(shard) > 0 {
		return 0
	}

	// 为了让每个节点在调用的时候获取到的配置是一样的
	//	这里将 gid 进行排序，确保遍历的顺序是确定的
	var gids []int
	for gid := range gidToShars {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	maxGid, maxShards := -1, -1
	for _, gid := range gids {
		if len(gidToShars[gid]) > maxShards {
			maxGid, maxShards = gid, len(gidToShars[gid])
		}
	}
	return maxGid
}

func gidWithMinShards(gidToShars map[int][]int) int {
	var gids []int
	for gid := range gidToShars {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	// midGid must be initialized with 0, for 0 stands for no gid.
	minGid, minShards := 0, NShards+1
	for _, gid := range gids {
		if gid != 0 && len(gidToShars[gid]) < minShards {
			minGid, minShards = gid, len(gidToShars[gid])
		}
	}
	return minGid
}
