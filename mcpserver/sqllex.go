package mcpserver

import (
	"errors"
	"strings"
)

// A small SQL lexer for the read-only guard: just enough of DuckDB's
// (PostgreSQL-derived) lexical grammar to know what is a string, a quoted
// identifier, a comment, a keyword and a semicolon. Anything it cannot close
// (an unterminated string or comment) is an error, never a guess.

type tokKind int

const (
	tokIdent       tokKind = iota // bare word; text is upper-cased
	tokQuotedIdent                // "..."; text is the unquoted name
	tokString                     // '...', E'...', $tag$...$tag$; text is the content
	tokNumber
	tokParam // $1
	tokPunct // any other single character
)

type token struct {
	kind tokKind
	text string
}

func (t token) isPunct(s string) bool { return t.kind == tokPunct && t.text == s }

// isName reports whether t can name a function.
func (t token) isName() bool { return t.kind == tokIdent || t.kind == tokQuotedIdent }

func lexSQL(s string) ([]token, error) {
	var toks []token
	for i := 0; i < len(s); {
		t, next, err := lexOne(s, i)
		if err != nil {
			return nil, err
		}
		if t != nil {
			toks = append(toks, *t)
		}
		i = next
	}
	return toks, nil
}

// lexOne reads the token (or whitespace/comment, yielding nil) at s[i].
func lexOne(s string, i int) (*token, int, error) {
	if next, skipped, err := skipTrivia(s, i); skipped {
		return nil, next, err
	}
	return lexToken(s, i)
}

// skipTrivia skips whitespace or one comment at s[i], reporting whether it did.
func skipTrivia(s string, i int) (int, bool, error) {
	switch {
	case isSpace(s[i]):
		return i + 1, true, nil
	case strings.HasPrefix(s[i:], "--"):
		return skipLineComment(s, i), true, nil
	case strings.HasPrefix(s[i:], "/*"):
		next, err := skipBlockComment(s, i)
		return next, true, err
	default:
		return i, false, nil
	}
}

func lexToken(s string, i int) (*token, int, error) {
	c := s[i]
	switch {
	case c == '\'':
		return lexQuoted(s, i, '\'', tokString)
	case c == '"':
		return lexQuoted(s, i, '"', tokQuotedIdent)
	case (c == 'e' || c == 'E') && i+1 < len(s) && s[i+1] == '\'':
		return lexEscapeString(s, i+1)
	case c == '$':
		return lexDollar(s, i)
	case isIdentStart(c):
		return lexWord(s, i)
	case isDigit(c):
		return lexNumber(s, i)
	default:
		return &token{kind: tokPunct, text: string(c)}, i + 1, nil
	}
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// isIdentStart treats every non-ASCII byte as a letter: DuckDB accepts
// Unicode identifiers, and a multi-byte rune is never one of the ASCII
// characters this lexer has to recognise.
func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c >= 0x80
}

func isIdentPart(c byte) bool { return isIdentStart(c) || isDigit(c) || c == '$' }

func skipLineComment(s string, i int) int {
	if j := strings.IndexByte(s[i:], '\n'); j >= 0 {
		return i + j + 1
	}
	return len(s)
}

// skipBlockComment skips a /* */ comment, which nests in DuckDB as in
// PostgreSQL: `/* a /* b */ SELECT */` is one comment.
func skipBlockComment(s string, i int) (int, error) {
	depth := 0
	for j := i; j+1 < len(s); j++ {
		switch s[j : j+2] {
		case "/*":
			depth++
			j++
		case "*/":
			depth--
			j++
			if depth == 0 {
				return j + 1, nil
			}
		}
	}
	return 0, errors.New("unterminated block comment")
}

// lexQuoted reads a '...' string or "..." identifier, where a doubled quote
// is an escaped one.
func lexQuoted(s string, i int, q byte, kind tokKind) (*token, int, error) {
	var b strings.Builder
	for j := i + 1; j < len(s); j++ {
		if s[j] != q {
			b.WriteByte(s[j])
			continue
		}
		if j+1 < len(s) && s[j+1] == q {
			b.WriteByte(q)
			j++
			continue
		}
		return &token{kind: kind, text: b.String()}, j + 1, nil
	}
	return nil, 0, errors.New("unterminated quoted string or identifier")
}

// lexEscapeString reads an E'...' string, where a backslash escapes the next
// character (so E'\” is one quote) and a doubled quote still works.
func lexEscapeString(s string, i int) (*token, int, error) {
	var b strings.Builder
	for j := i + 1; j < len(s); j++ {
		switch {
		case s[j] == '\\' && j+1 < len(s):
			b.WriteByte(s[j+1])
			j++
		case s[j] == '\'' && j+1 < len(s) && s[j+1] == '\'':
			b.WriteByte('\'')
			j++
		case s[j] == '\'':
			return &token{kind: tokString, text: b.String()}, j + 1, nil
		default:
			b.WriteByte(s[j])
		}
	}
	return nil, 0, errors.New("unterminated escape string")
}

// lexDollar reads a positional parameter ($1) or a dollar-quoted string
// ($$...$$ or $tag$...$tag$). A lone $ is punctuation.
func lexDollar(s string, i int) (*token, int, error) {
	j := i + 1
	if j < len(s) && isDigit(s[j]) {
		for j < len(s) && isDigit(s[j]) {
			j++
		}
		return &token{kind: tokParam, text: s[i:j]}, j, nil
	}
	for j < len(s) && s[j] != '$' && isIdentPart(s[j]) {
		j++
	}
	if j >= len(s) || s[j] != '$' {
		return &token{kind: tokPunct, text: "$"}, i + 1, nil
	}
	return lexDollarQuoted(s, s[i:j+1], j+1)
}

// lexDollarQuoted reads the body of a dollar-quoted string opened by delim,
// starting at s[body].
func lexDollarQuoted(s, delim string, body int) (*token, int, error) {
	end := strings.Index(s[body:], delim)
	if end < 0 {
		return nil, 0, errors.New("unterminated dollar-quoted string")
	}
	return &token{kind: tokString, text: s[body : body+end]}, body + end + len(delim), nil
}

func lexWord(s string, i int) (*token, int, error) {
	j := i
	for j < len(s) && isIdentPart(s[j]) {
		j++
	}
	return &token{kind: tokIdent, text: strings.ToUpper(s[i:j])}, j, nil
}

func lexNumber(s string, i int) (*token, int, error) {
	j := i
	for j < len(s) && (isDigit(s[j]) || s[j] == '.' || s[j] == '_' || s[j] == 'e' || s[j] == 'E') {
		j++
	}
	return &token{kind: tokNumber, text: s[i:j]}, j, nil
}
