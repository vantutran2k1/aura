package queryparser

import (
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

type (
	Value struct {
		String     *string  `  @String`
		Identifier *string  `| @Ident`
		Number     *float64 `| @Float | @Int`
	}

	Term struct {
		Field    string `@Ident`
		Operator string `@("=" | "!=" | "LIKE" | ">" | "<" | ">=" | "<=")`
		Value    *Value `@@`
	}

	OpExpression struct {
		Operator   string      `@("AND" | "OR")`
		Expression *Expression `@@`
	}

	Expression struct {
		Left  *Term           `@@`
		Right []*OpExpression `@@*`
	}

	AST struct {
		Expression *Expression `@@`
	}
)

var (
	queryLexer = lexer.MustSimple([]lexer.SimpleRule{
		{Name: "Ident", Pattern: `[a-zA-Z_][a-zA-Z0-9_\-\.]*`},
		{Name: "String", Pattern: `"[^"]*"`},
		{Name: "Float", Pattern: `\d+\.\d+`},
		{Name: "Int", Pattern: `\d+`},
		{Name: "Operators", Pattern: `(!=|=|>|<|>=|<=|LIKE|AND|OR)`},
		{Name: "Punct", Pattern: `[\(\)]`},
		{Name: "Whitespace", Pattern: `\s+`},
	})

	queryParser = participle.MustBuild[AST](
		participle.Lexer(queryLexer),
		participle.Elide("Whitespace"),
	)
)

func Parse(query string) (*AST, error) {
	return queryParser.ParseString("", query)
}
