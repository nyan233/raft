package raft

type membershipData struct {
	Me       string   `json:"me"`
	LogIndex uint64   `json:"log_index"`
	Term     uint64   `json:"term"`
	List     []string `json:"list"`
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
