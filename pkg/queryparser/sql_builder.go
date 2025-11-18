package queryparser

import (
	"fmt"
	"strings"
)

func BuildSQL(ast *AST) (string, []any, error) {
	var sql strings.Builder
	var args []any

	if ast.Expression == nil {
		return "", nil, fmt.Errorf("empty query")
	}

	err := walkExpression(&sql, &args, ast.Expression)
	if err != nil {
		return "", nil, err
	}

	return sql.String(), args, nil
}

func walkExpression(sql *strings.Builder, args *[]any, expr *Expression) error {
	if err := walkTerm(sql, args, expr.Left); err != nil {
		return err
	}

	for _, right := range expr.Right {
		sql.WriteString(fmt.Sprintf(" %s ", right.Operator))

		if err := walkExpression(sql, args, right.Expression); err != nil {
			return err
		}
	}

	return nil
}

func walkTerm(sql *strings.Builder, args *[]any, term *Term) error {
	op := strings.ToUpper(term.Operator)
	switch op {
	case "=", "!=", "LIKE", ">", "<", ">=", "<=":
	default:
		return fmt.Errorf("invalid operator: %s", term.Operator)
	}

	sql.WriteString(fmt.Sprintf("%s %s ?", term.Field, op))

	if term.Value.String != nil {
		cleanString := strings.Trim(*term.Value.String, `"`)
		*args = append(*args, cleanString)
	} else if term.Value.Identifier != nil {
		*args = append(*args, *term.Value.Identifier)
	} else if term.Value.Number != nil {
		*args = append(*args, *term.Value.Number)
	} else {
		return fmt.Errorf("invalid value for field %s", term.Field)
	}

	return nil
}
