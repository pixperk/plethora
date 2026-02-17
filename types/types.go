package types

type Key string

type Value struct {
	Data    string
	Context Context
}

type Context struct {
	Version uint32
	NodeID  string
}
