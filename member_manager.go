package raft

import (
	"slices"

	"github.com/nyan233/littlerpc/core/common/context"
)

type memberManager struct {
	my     string
	leader string
	l      []string
}

func (m *memberManager) intiMember(my string, l []string) {
	m.my = my
	m.l = []string{my}
	m.l = append(m.l, l...)
}

func (m *memberManager) append(v string) {
	m.l = append(m.l, v)
}

func (m *memberManager) remove(v string) {
	m.l = slices.DeleteFunc(m.l, func(s string) bool {
		return s == v
	})
}

func (m *memberManager) getList() []string {
	r := make([]string, 0, len(m.l))
	r = append(r, m.l...)
	return r
}

func (m *memberManager) getMy() string {
	return m.my
}

func (m *memberManager) quorum() int {
	return len(m.l)/2 + 1
}

func (m *memberManager) broadcastRequest(ctx *context.Context) {

}

func (m *memberManager) setLeader(leader string) {
	m.leader = leader
}

func (m *memberManager) getLeader() string {
	return m.leader
}
