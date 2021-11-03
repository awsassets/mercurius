package mercurius

type Application interface {
	Prepare(sn uint64, key uint32, content string) error
	Commit(sn uint64, key uint32) error
	Acknowledge(sn uint64, key uint32) error
}

type CommitRequest struct {
	Key     uint32
	Content string
}
