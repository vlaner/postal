package schema

import (
	"bufio"
	"fmt"
	"unicode"
)

type Token struct {
	typ TokenType
	val string
}

var KEYWORDS = map[string]TokenType{"int": TOKEN_LITERAL, "str": TOKEN_LITERAL}

type Lexer struct {
	r   *bufio.Reader
	cur rune
	eof bool
}

func NewLexer(r *bufio.Reader) *Lexer {
	l := &Lexer{r: r}
	l.readChar()
	return l
}

func (l *Lexer) tokenize() ([]Token, error) {
	var tokens []Token

	var t Token
	var err error
	for t.typ != TOKEN_EOF {
		t, err = l.nextToken()
		if err != nil {
			return tokens, err
		}

		tokens = append(tokens, t)
	}

	return tokens, nil
}

func (l *Lexer) nextToken() (Token, error) {
	l.readWhiteSpace()

	if l.eof {
		return Token{typ: TOKEN_EOF, val: ""}, nil
	}

	switch l.cur {
	case '[':
		l.readChar()
		return Token{typ: TOKEN_LBRACKET, val: "["}, nil
	case ']':
		l.readChar()
		return Token{typ: TOKEN_RBRACKET, val: "]"}, nil
	case '>':
		l.readChar()
		return Token{typ: TOKEN_RIGHT_ARROW, val: ">"}, nil
	default:
		if isIdentifier(l.cur) {
			token := l.readIdentifier()
			if typ, ok := toKeyword(token.val); ok {
				token.typ = typ
			}

			return token, nil
		}

		return Token{}, fmt.Errorf("unexpected token: %s - %v", string(l.cur), l.cur)
	}
}

func (l *Lexer) readIdentifier() Token {
	var ident []rune
	for !l.eof && isIdentifier(l.cur) {
		ident = append(ident, l.cur)
		l.readChar()
	}

	return Token{typ: TOKEN_IDENTIFIER, val: string(ident)}
}

func (l *Lexer) readWhiteSpace() {
	for !l.eof && unicode.IsSpace(l.cur) {
		l.readChar()
	}
}

func (l *Lexer) readChar() {
	r, _, err := l.r.ReadRune()
	if err != nil {
		l.eof = true
		return
	}

	l.cur = r
}

func isIdentifier(r rune) bool {
	return unicode.IsDigit(r) || unicode.IsLetter(r) || r == '_'
}

func toKeyword(s string) (TokenType, bool) {
	typ, ok := KEYWORDS[s]
	return typ, ok
}
