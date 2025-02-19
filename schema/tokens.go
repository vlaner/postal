package schema

type TokenType string

var (
	TOKEN_RBRACKET    TokenType = "TOKEN_RBRACKET"    // [
	TOKEN_LBRACKET    TokenType = "TOKEN_LBRACKET"    // ]
	TOKEN_RIGHT_ARROW TokenType = "TOKEN_RIGHT_ARROW" // >
	TOKEN_IDENTIFIER  TokenType = "TOKEN_IDENTIFIER"  // key name
	TOKEN_LITERAL     TokenType = "TOKEN_LITERAL"
	TOKEN_EOF         TokenType = "EOF"
)
