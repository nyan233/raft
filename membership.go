package raft

type membershipData struct {
	me       string
	logIndex uint64
	term     uint64
	list     []string
}

type membership struct {
	jf *jsonFile[membershipData]
}

func newMembership(path string) *membership {
	return &membership{
		jf: newJSONFile[membershipData](path),
	}
}

func (m *membership) init() error {
	return m.jf.init()
}

func (m *membership) get() *membershipData {
	return m.jf.get()
}

func (m *membership) save(v membershipData) error {
	return m.jf.save(v)
}
