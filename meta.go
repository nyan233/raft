package raft

type metaData struct {
	VoteFor string `json:"vote_for"`
	Term    uint64 `json:"term"`
}

type meta struct {
	d    *metaData
	path string
}

func newMeta(path string) *meta {
	return &meta{
		path: path,
	}
}

func (m *meta) close() error {
	m.d = nil
	return nil
}

func (m *meta) get() *metaData {
	return m.d
}

func (m *meta) termIncr() (uint64, error) {
	d := *m.get()
	d.Term++
	err := m.save(d)
	if err != nil {
		return 0, err
	}
	return m.get().Term, nil
}
