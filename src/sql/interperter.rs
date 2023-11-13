use super::error::Error;
use super::tokenizer::Token;
use super::{ColumnDef, Ordering};
use crate::engine::structure::Value;
use crate::sql::Statement;

type TokenIter<'a> = std::iter::Peekable<std::slice::Iter<'a, Token>>;

macro_rules! next_token {
    ($tokens:ident) => {
        $tokens
            .next()
            .ok_or_else(|| Error::Systax("Did not expect EOL"))?
    };
}

macro_rules! peek_next {
    ($tokens:ident) => {
        $tokens
            .peek()
            .ok_or_else(|| Error::Systax("Did not expext EOL"))?
    };
}

pub fn interpect(buffer: Vec<Token>) -> Result<Statement, Error> {
    let mut list = buffer.iter().peekable();

    let index = list.next().ok_or_else(|| Error::Systax("Invaild token"))?;

    match index {
        Token::Ident(value) => match value.to_lowercase().as_str() {
            "insert" => parse_insert(&mut list),
            "create" => parse_create_table(&mut list),
            "select" => parse_select(&mut list),
            _ => Err(Error::Systax("Expected insert,create,or select")),
        },
        _ => Err(Error::Systax("Expected identifer")),
    }
}

fn parse_create_table(tokens: &mut TokenIter<'_>) -> Result<Statement, Error> {
    let mut table_cols = vec![];

    if !next_token!(tokens).is_keyword("table") {
        return Err(Error::Systax(
            "Invaild systax: Expected 'table'|'index'|'trigger'|'view'| after 'create'",
        ));
    }

    let table_name = match next_token!(tokens).get_identifer() {
        Some(ident) => ident,
        None => return Err(Error::Systax("Invaild systax: Expected a table name")),
    };

    if !next_token!(tokens).is_token(&Token::LeftPren) {
        return Err(Error::Systax("Invaild token"));
    }

    let mut primary_key_idx: usize = 0;
    let mut commas = 0;
    while let Some(value) = tokens.next() {
        match value {
            Token::Ident(ident) => {
                if let Some(data_type) = tokens.next() {
                    let token = match data_type.get_identifer() {
                        Some(i) => i,
                        _ => {
                            return Err(Error::Systax(
                                "Invaild systax: Expected a data type after table col name",
                            ))
                        }
                    };

                    if table_cols.len() != commas {
                        return Err(Error::Systax("Expected a ','"));
                    }

                    let c = Value::from_string(&token.to_lowercase());

                    let mut autoincrement = false;
                    let mut ordering = Ordering::default();
                    let mut unique = false;
                    let mut nullable = true;
                    while let Some(v) = tokens.peek() {
                        if v == &&Token::Comma || v == &&Token::RightPren || v == &&Token::EOL {
                            break;
                        }
                        match parse_column_constraint(tokens)? {
                            ColumnConstraint::PrimaryKey(row_ordering, autointer) => {
                                nullable = false;
                                unique = true;
                                primary_key_idx = commas;
                                ordering = row_ordering;
                                autoincrement = autointer;
                            }
                            ColumnConstraint::NotNull => {
                                nullable = false;
                            }
                            ColumnConstraint::Unique => {
                                unique = true;
                            }
                            ColumnConstraint::Default(_) => todo!(),
                            ColumnConstraint::None => {}
                        };
                    }

                    table_cols.push(ColumnDef::new(
                        ident.to_owned(),
                        nullable,
                        c,
                        autoincrement,
                        ordering,
                        None,
                    ));
                }
            }
            Token::Comma => commas += 1,
            Token::RightPren => {
                break;
            }
            _ => {
                return Err(Error::Systax(
                    "Invaild token: Expected a comma or right pren.",
                ));
            }
        }
    }

    if !next_token!(tokens).is_token(&Token::SemiComma) {
        return Err(Error::Systax(
            "Invaild token: Expected to end with as semicomma",
        ));
    }

    Ok(Statement::Create {
        primary_key: primary_key_idx,
        table: table_name,
        cols: table_cols,
    })
}

#[derive(Debug, PartialEq)]
pub enum ColumnData {
    Null,
    Value(String),
}

fn parse_insert(tokens: &mut TokenIter<'_>) -> Result<Statement, Error> {
    let mut cols: Vec<String> = vec![];
    let mut data: Vec<ColumnData> = vec![];

    if !next_token!(tokens).is_keyword("into") {
        return Err(Error::Systax("Expected keyword 'into'."));
    }

    let table_name = match next_token!(tokens).get_identifer() {
        Some(i) => i,
        _ => return Err(Error::Systax("Invaild token")),
    };

    // parse
    // TABLE (id,name,user) values
    // or
    // TABLE values
    match next_token!(tokens) {
        Token::LeftPren => {
            let mut commas: usize = 0;
            while let Some(value) = tokens.next() {
                match value {
                    Token::Ident(ident) => {
                        if cols.len() != commas {
                            return Err(Error::Systax("Expected an comma"));
                        }

                        cols.push(ident.to_owned());
                    }
                    Token::Comma => commas += 1,
                    Token::RightPren => break,
                    _ => return Err(Error::Systax("Invaild token")),
                }
            }

            if !next_token!(tokens).is_keyword("values") {
                return Err(Error::Systax("Expected keyword 'values'"));
            }
        }
        Token::Ident(value) => {
            if value.to_lowercase() != "values" {
                return Err(Error::Systax("Expected keyword 'values'"));
            }
        }

        _ => return Err(Error::Systax("Expected keyword 'values' or '('")),
    }

    match next_token!(tokens) {
        Token::LeftPren => {
            let mut commas: usize = 0;
            while let Some(value) = tokens.next() {
                match value {
                    Token::Ident(value) => {
                        if data.len() != commas {
                            return Err(Error::Systax("Expected an comma"));
                        }

                        if value.to_lowercase().as_str() != "null" {
                            return Err(Error::Systax("did not expected identifier"));
                        }

                        data.push(ColumnData::Null);
                    }
                    Token::Number(value) => {
                        if data.len() != commas {
                            return Err(Error::Systax("Expected an comma"));
                        }
                        data.push(ColumnData::Value(value.to_owned()));
                    }
                    Token::String(value) => {
                        if data.len() != commas {
                            return Err(Error::Systax("Expected an comma"));
                        }
                        data.push(ColumnData::Value(value.to_owned()));
                    }
                    Token::Comma => commas += 1,
                    Token::RightPren => break,
                    _ => return Err(Error::Systax("Invaild token")),
                }
            }
        }
        _ => return Err(Error::Systax("Expected '('")),
    }

    if !next_token!(tokens).is_token(&Token::SemiComma) {
        return Err(Error::Systax("Expected semicolon"));
    }

    Ok(Statement::Insert {
        table: table_name,
        cols,
        data,
    })
}

fn parse_select(tokens: &mut TokenIter<'_>) -> Result<Statement, Error> {
    let mut cols = vec![];

    match next_token!(tokens) {
        Token::LeftPren => {
            let mut commas = 0;
            while let Some(value) = tokens.next() {
                match value {
                    Token::Ident(ident) => {
                        if cols.len() != commas {
                            return Err(Error::Systax("Expected an comma"));
                        }

                        cols.push(ident.to_owned());
                    }
                    Token::Comma => {
                        commas += 1;
                    }
                    Token::RightPren => break,
                    _ => return Err(Error::Systax("Expected an column name, ')' or ','.")),
                }
            }
        }
        Token::Star => {}
        _ => return Err(Error::Systax("Expected '*' or '('")),
    }

    if !next_token!(tokens).is_keyword("from") {
        return Err(Error::Systax("Expected keyword 'from'"));
    }

    let table_name = match next_token!(tokens).get_identifer() {
        Some(i) => i,
        None => return Err(Error::Systax("Invaild token")),
    };

    if !next_token!(tokens).is_token(&Token::SemiComma) {
        return Err(Error::Systax("Expected ';'"));
    }

    Ok(Statement::Select {
        table: table_name,
        columns: cols,
    })
}

#[derive(Debug, PartialEq)]
pub enum ColumnConstraint {
    PrimaryKey(Ordering, bool),
    NotNull,
    Unique,
    Default(String),
    None,
}
/// Handle parseing column constraints
/// see - https://www.sqlite.org/syntax/column-constraint.html for flow chart.
///
/// Handles the following
///
/// PRIMARY KEY  (ACS|DESC)? conflict-clause (AUTOINCREMENT)?
/// NOT NULL conflict-clause
/// DEFAULT literal-value|signed-number
fn parse_column_constraint(tokens: &mut TokenIter<'_>) -> Result<ColumnConstraint, Error> {
    match peek_next!(tokens) {
        Token::Ident(item) => match item.to_lowercase().as_str() {
            "primary" => {
                tokens.next();

                if !next_token!(tokens).is_keyword("key") {
                    return Err(Error::Systax("Expected keyworld 'key' after 'primary'"));
                }

                let mut orderering = Ordering::default();
                if let Token::Ident(value) = tokens.peek().unwrap_or(&&Token::EOL) {
                    orderering = match value.to_lowercase().as_str() {
                        "asc" => {
                            tokens.next();
                            Ordering::Asc
                        }
                        "desc" => {
                            tokens.next();
                            Ordering::Desc
                        }
                        _ => Ordering::default(),
                    }
                }

                let autoint = if let Token::Ident(value) = tokens.peek().unwrap_or(&&Token::EOL) {
                    match value.to_lowercase().as_str() == "autoincrement" {
                        true => {
                            tokens.next();
                            true
                        }
                        false => false,
                    }
                } else {
                    false
                };

                return Ok(ColumnConstraint::PrimaryKey(orderering, autoint));
            }
            "not" => {
                tokens.next();
                if !next_token!(tokens).is_keyword("null") {
                    return Err(Error::Systax("Expected keyworld 'null' after 'not'"));
                }
                return Ok(ColumnConstraint::NotNull);
            }
            "unique" => {
                tokens.next();
                return Ok(ColumnConstraint::Unique);
            }
            "default" => {
                tokens.next();
                match next_token!(tokens) {
                    Token::Number(num) => Ok(ColumnConstraint::Default(num.to_owned())),
                    Token::String(string) => Ok(ColumnConstraint::Default(string.to_owned())),
                    _ => Err(Error::Systax("Invaild data")),
                }
            }
            _ => Ok(ColumnConstraint::None),
        },
        _ => Ok(ColumnConstraint::None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{
        interperter::{ColumnConstraint, Ordering},
        tokenizer::Token,
    };

    use super::ColumnData;

    #[test]
    fn test_column_constarint_primary_key() {
        let primary_key = vec![Token::Ident("PRIMARY".into()), Token::Ident("KEY".into())];
        let mut primary_iter = primary_key.iter().peekable();

        match parse_column_constraint(&mut primary_iter) {
            Ok(value) => {
                assert_eq!(value, ColumnConstraint::PrimaryKey(Ordering::Asc, false))
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_column_constarint_primary_key_autoint() {
        let primary_key = vec![
            Token::Ident("PRIMARY".into()),
            Token::Ident("KEY".into()),
            Token::Ident("AUTOINCREMENT".into()),
        ];
        let mut primary_iter = primary_key.iter().peekable();

        match parse_column_constraint(&mut primary_iter) {
            Ok(value) => {
                assert_eq!(value, ColumnConstraint::PrimaryKey(Ordering::Asc, true))
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_column_constarint_primary_key_desc() {
        let primary_key = vec![
            Token::Ident("PRIMARY".into()),
            Token::Ident("KEY".into()),
            Token::Ident("DESC".into()),
        ];
        let mut primary_iter = primary_key.iter().peekable();

        match parse_column_constraint(&mut primary_iter) {
            Ok(value) => {
                assert_eq!(value, ColumnConstraint::PrimaryKey(Ordering::Desc, false))
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_column_constarint_not_null() {
        let primary_key = vec![Token::Ident("NOT".into()), Token::Ident("NULL".into())];
        let mut primary_iter = primary_key.iter().peekable();

        match parse_column_constraint(&mut primary_iter) {
            Ok(value) => {
                assert_eq!(value, ColumnConstraint::NotNull)
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_column_constarint_unique() {
        let primary_key = vec![Token::Ident("UNIQUE".into())];
        let mut primary_iter = primary_key.iter().peekable();

        match parse_column_constraint(&mut primary_iter) {
            Ok(value) => {
                assert_eq!(value, ColumnConstraint::Unique)
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_interpect() {
        let tokens = vec![
            super::Token::Ident("insert".to_string()),
            super::Token::Ident("into".to_string()),
            super::Token::Ident("TABLE".to_string()),
            super::Token::LeftPren,
            super::Token::Ident("id".to_string()),
            super::Token::Comma,
            super::Token::Ident("username".to_string()),
            super::Token::RightPren,
            super::Token::Ident("values".to_string()),
            super::Token::LeftPren,
            super::Token::Number("1".to_string()),
            super::Token::Comma,
            super::Token::String("test-user".to_string()),
            super::Token::RightPren,
            super::Token::SemiComma,
        ];

        let statement = super::interpect(tokens);

        if let Ok(state) = statement {
            assert_eq!(
                super::Statement::Insert {
                    table: "TABLE".to_string(),
                    cols: vec!["id".to_string(), "username".to_string()],
                    data: vec![
                        ColumnData::Value("1".to_string()),
                        ColumnData::Value("test-user".to_string())
                    ]
                },
                state
            );
        }
    }
}
