package schema

import (
	"bufio"
	"bytes"
	"fmt"
)

type Parser struct {
	tokens  []Token
	current Token
}

func NewParser(tokens []Token) *Parser {
	p := &Parser{tokens: tokens}
	p.readToken()

	return p
}

func NewParserString(input string) (*Parser, error) {
	b := new(bytes.Buffer)
	_, err := b.WriteString(input)
	if err != nil {
		return nil, fmt.Errorf("write to buffer: %w", err)
	}

	l := NewLexer(bufio.NewReader(b))
	tokens, err := l.tokenize()
	if err != nil {
		return nil, fmt.Errorf("lexer: %w", err)
	}
	p := &Parser{
		tokens: tokens,
	}
	p.readToken()

	return p, nil
}

func (p *Parser) Parse() (NodeSchema, error) {
	if p.current.typ != TOKEN_LBRACKET {
		return NodeSchema{}, fmt.Errorf("current token is not '[' (left bracket): %s", p.current.typ)
	}
	p.readToken()

	ps := NodeSchema{body: []NodeAssign{}}

	for p.current.typ != TOKEN_EOF && p.current.typ != TOKEN_RBRACKET {
		assign, err := p.parseAssignment()
		if err != nil {
			return NodeSchema{}, err
		}
		ps.body = append(ps.body, assign)
	}

	if p.current.typ != TOKEN_RBRACKET {
		return NodeSchema{}, fmt.Errorf("current token is not ']' (right bracket): %s", p.current.typ)
	}

	return ps, nil
}

func (p *Parser) readToken() {
	p.current = p.tokens[0]
	p.tokens = p.tokens[1:]
}

func (p *Parser) parseAssignment() (NodeAssign, error) {
	if p.current.typ != TOKEN_IDENTIFIER {
		return NodeAssign{}, fmt.Errorf("%s is not identifier", p.current.typ)
	}

	n := NodeAssign{ident: NodeIdent{name: p.current.val}}
	p.readToken() // consume identifier

	if p.current.typ != TOKEN_RIGHT_ARROW {
		return NodeAssign{}, fmt.Errorf("%s is not right arrow", p.current.typ)
	}
	p.readToken() // consume >

	var node Node
	switch p.current.typ {
	case TOKEN_LITERAL:
		node = NodeLiteral{name: p.current.val}
		n.val = node
		p.readToken()

	case TOKEN_LBRACKET:
		nested, err := p.Parse()
		if err != nil {
			return NodeAssign{}, nil
		}
		n.val = nested
	}

	return n, nil
}
