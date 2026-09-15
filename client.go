package raft

import (
	"errors"
	"github.com/nyan233/littlerpc/core/client"
	"github.com/nyan233/littlerpc/core/common/context"
	"github.com/nyan233/littlerpc/core/middle/ns"
	"github.com/nyan233/raft/pb/message/raft"
	"strings"
	"sync"
)

type Client struct {
	mu         sync.Mutex
	leaderIp   string
	initNodeIp string
	proxy      raft.RaftProxy
}

func NewClient(nodeIp string) (*Client, error) {
	c := &Client{
		initNodeIp: nodeIp,
	}
	rpcc, err := client.New(
		//client.WithMuxWriter(),
		client.WithNsStorage(ns.NewFixedStorage([]string{nodeIp})),
	)
	if err != nil {
		return nil, err
	}
	c.proxy = raft.NewRaft(rpcc)
	return c, nil
}

func (c *Client) AppendCommands(ctx *context.Context, commands [][]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.doAppendCommands(ctx, commands)
}

func (c *Client) doAppendCommands(ctx *context.Context, commands [][]byte) error {
	if c.leaderIp == "" {
		rsp, err := c.proxy.GetLeader(ctx, &raft.GetLeaderReq{}, client.WithAddr(c.initNodeIp))
		if err != nil {
			return err
		}
		if rsp.LeaderIp == "" {
			return errors.New("no leader")
		}
		c.leaderIp = rsp.LeaderIp
	}
	_, err := c.proxy.AppendCommands(ctx, &raft.AppendCommandsReq{
		Commands: commands,
	}, client.WithAddr(c.leaderIp))
	if err != nil {
		if strings.HasPrefix(err.Error(), "my is not leader") {
			c.leaderIp = ""
		}
		return err
	}
	return nil
}
