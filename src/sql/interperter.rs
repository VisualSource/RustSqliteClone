use super::error::Error;
use super::tokenizer::Token;
use crate::commands::Statement;
use crate::engine::structure::Value;

macro_rules! next_token {
    ($tokens:ident) => {
        $tokens
            .next()
            .ok_or_else(|| Error::Systax("Did not expect EOL"))?
    };
}

pub fn interpect(buffer: Vec<Token>) -> Result<Statement, Error> {
    let mut list = buffer.iter();

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

fn parse_create_table(tokens: &mut std::slice::Iter<'_, Token>) -> Result<Statement, Error> {
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

                    table_cols.push((ident.to_owned(), c, false));
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
        primary_key: 0,
        table: table_name,
        cols: table_cols,
    })
}

fn parse_insert(tokens: &mut std::slice::Iter<'_, Token>) -> Result<Statement, Error> {
    let mut cols: Vec<String> = vec![];
    let mut data: Vec<String> = vec![];

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
                    Token::Number(value) => {
                        if data.len() != commas {
                            return Err(Error::Systax("Expected an comma"));
                        }
                        data.push(value.to_owned());
                    }
                    Token::String(value) => {
                        if data.len() != commas {
                            return Err(Error::Systax("Expected an comma"));
                        }
                        data.push(value.to_owned());
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

fn parse_select(tokens: &mut std::slice::Iter<'_, Token>) -> Result<Statement, Error> {
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

#[cfg(test)]
mod tests {

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
                    data: vec!["1".to_string(), "test-user".to_string()]
                },
                state
            );
        }
    }
}
