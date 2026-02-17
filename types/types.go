package types

type Key string

type Value struct {
	ValueName string
	Context   Context
}

type Context struct {
	Version uint32
	NodeID  string
}
