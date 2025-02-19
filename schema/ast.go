package schema

import "fmt"

type NodeType string

const (
	NODE_ASSIGN  NodeType = "NODE_ASSIGN"
	NODE_IDENT   NodeType = "NODE_IDENT"
	NODE_LITERAL NodeType = "NODE_LITERAL"
	NODE_SCHEMA  NodeType = "NODE_SCHEMA"
)

type Node interface {
	Type() NodeType
	String() string
}

type NodeAssign struct {
	ident Node
	val   Node
}

func (n NodeAssign) Type() NodeType {
	return NODE_ASSIGN
}

func (n NodeAssign) String() string {
	return fmt.Sprintf("%s > %s", n.ident, n.val)
}

type NodeIdent struct {
	name string
}

func (n NodeIdent) Type() NodeType {
	return NODE_IDENT
}

func (n NodeIdent) String() string {
	return n.name
}

type NodeLiteral struct {
	name string
}

func (n NodeLiteral) Type() NodeType {
	return NODE_LITERAL
}

func (n NodeLiteral) String() string {
	return n.name
}

type NodeSchema struct {
	body []NodeAssign
}

func (n NodeSchema) Type() NodeType {
	return NODE_SCHEMA
}

func (n NodeSchema) String() string {
	return "body"
}
