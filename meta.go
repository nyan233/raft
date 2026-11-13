package raft

type metaData struct {
	VoteFor string `json:"vote_for"`
	Term    uint64 `json:"term"`
}

type meta struct {
	jf *jsonFile[metaData]
}

func newMeta(path string) *meta {
	return &meta{
		jf: newJSONFile[metaData](path),
	}
}

func (m *meta) init() error {
	return m.jf.init()
}

func (m *meta) close() error {
	return nil
}

func (m *meta) get() *metaData {
	return m.jf.get()
}

func (m *meta) save(md metaData) error {
	return m.jf.save(md)
}

func (m *meta) termIncr() (uint64, error) {
	d := *m.get()
	d.Term++
	err := m.jf.save(d)
	if err != nil {
		return 0, err
	}
	return m.get().Term, nil
}
