use crate::{
    commands::Statement,
    errors::{DBError, DatabaseError},
    structure::Col,
};

macro_rules! next_token {
    ($tokens:ident) => {
        $tokens
            .next()
            .ok_or_else(|| DatabaseError::SystaxError("Did not expect EOL"))?
    };
}

#[derive(Debug, PartialEq)]
pub enum Token {
    Comma,
    SemiComma,
    RightPren,
    LeftPren,
    Period,
    Star,
    String(String),
    Number(String),
    Ident(String),
}

impl Token {
    fn is_keyword(&self, keyword: &'static str) -> bool {
        match &self {
            Self::Ident(value) => value.to_lowercase() == keyword,
            _ => false,
        }
    }

    fn is_token(&self, token: &Token) -> bool {
        self == token
    }

    fn get_identifer(&self) -> Option<String> {
        match self {
            Self::Ident(value) => Some(value.to_owned()),
            _ => None,
        }
    }
}

pub fn tokenizer(buffer: &String) -> DBError<Vec<Token>> {
    let mut input = buffer.chars().peekable();

    let mut tokens = vec![];
    while let Some(char) = input.next() {
        match char {
            e if e.is_control() => continue,
            e if e.is_whitespace() => continue,
            e if e.is_numeric() => {
                let mut value = String::default();

                value.push(e);

                while let Some(item) = input.peek() {
                    if !item.is_numeric() {
                        break;
                    }

                    if let Some(c) = input.next() {
                        value.push(c);
                    }
                }

                tokens.push(Token::Number(value))
            }
            'a'..='z' | 'A'..='Z' | '_' => {
                let mut value = String::default();

                value.push(char);

                while let Some(item) = input.peek() {
                    if !item.is_alphabetic() {
                        break;
                    }
                    if let Some(c) = input.next() {
                        value.push(c);
                    }
                }

                tokens.push(Token::Ident(value));
            }
            '\"' => {
                let mut value = String::default();
                while let Some(item) = input.peek() {
                    if item == &'\"' {
                        input.next();
                        break;
                    }

                    if let Some(c) = input.next() {
                        value.push(c);
                    }
                }

                tokens.push(Token::String(value))
            }
            '(' => tokens.push(Token::LeftPren),
            ')' => tokens.push(Token::RightPren),
            '.' => tokens.push(Token::Period),
            '*' => tokens.push(Token::Star),
            ';' => tokens.push(Token::SemiComma),
            ',' => tokens.push(Token::Comma),
            _ => {
                return Err(DatabaseError::TokenizerError(format!(
                    ": Unknown char: {}",
                    char.escape_debug()
                )))
            }
        }
    }

    Ok(tokens)
}

pub fn interpect(buffer: Vec<Token>) -> DBError<Statement> {
    let mut list = buffer.iter();

    let index = list
        .next()
        .ok_or_else(|| DatabaseError::SystaxError("Invaild token"))?;

    match index {
        Token::Ident(value) => match value.to_lowercase().as_str() {
            "insert" => parse_insert(&mut list),
            "create" => parse_create_table(&mut list),
            "select" => parse_select(&mut list),
            _ => Err(DatabaseError::SystaxError(
                "Expected insert,create,or select",
            )),
        },
        _ => Err(DatabaseError::SystaxError("Expected identifer")),
    }
}

fn parse_create_table(tokens: &mut std::slice::Iter<'_, Token>) -> DBError<Statement> {
    let mut table_cols = vec![];

    if !next_token!(tokens).is_keyword("table") {
        return Err(DatabaseError::SystaxError(
            "Invaild systax: Expected 'table'|'index'|'trigger'|'view'| after 'create'",
        ));
    }

    let table_name = match next_token!(tokens).get_identifer() {
        Some(ident) => ident,
        None => {
            return Err(DatabaseError::SystaxError(
                "Invaild systax: Expected a table name",
            ))
        }
    };

    if !next_token!(tokens).is_token(&Token::LeftPren) {
        return Err(DatabaseError::SystaxError("Invaild token"));
    }

    let mut commas = 0;
    while let Some(value) = tokens.next() {
        match value {
            Token::Ident(ident) => {
                if let Some(data_type) = tokens.next() {
                    let token = match data_type.get_identifer() {
                        Some(i) => i,
                        _ => {
                            return Err(DatabaseError::SystaxError(
                                "Invaild systax: Expected a data type after table col name",
                            ))
                        }
                    };

                    if table_cols.len() != commas {
                        return Err(DatabaseError::SystaxError("Expected a ','"));
                    }

                    let c = token.try_into()?;

                    let col = Col {
                        name: ident.to_owned(),
                        data_type: c,
                    };

                    table_cols.push(col);
                }
            }
            Token::Comma => commas += 1,
            Token::RightPren => {
                break;
            }
            _ => {
                return Err(DatabaseError::SystaxError(
                    "Invaild token: Expected a comma or right pren.",
                ));
            }
        }
    }

    if !next_token!(tokens).is_token(&Token::SemiComma) {
        return Err(DatabaseError::SystaxError(
            "Invaild token: Expected to end with as semicomma",
        ));
    }

    Ok(Statement::Create {
        table: table_name,
        cols: table_cols,
    })
}

fn parse_insert(tokens: &mut std::slice::Iter<'_, Token>) -> DBError<Statement> {
    let mut cols: Vec<String> = vec![];
    let mut data: Vec<String> = vec![];

    if !next_token!(tokens).is_keyword("into") {
        return Err(DatabaseError::SystaxError("Expected keyword 'into'."));
    }

    let table_name = match next_token!(tokens).get_identifer() {
        Some(i) => i,
        _ => return Err(DatabaseError::SystaxError("Invaild token")),
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
                            return Err(DatabaseError::SystaxError("Expected an comma"));
                        }

                        cols.push(ident.to_owned());
                    }
                    Token::Comma => commas += 1,
                    Token::RightPren => break,
                    _ => return Err(DatabaseError::SystaxError("Invaild token")),
                }
            }

            if !next_token!(tokens).is_keyword("values") {
                return Err(DatabaseError::SystaxError("Expected keyword 'values'"));
            }
        }
        Token::Ident(value) => {
            if value.to_lowercase() != "values" {
                return Err(DatabaseError::SystaxError("Expected keyword 'values'"));
            }
        }

        _ => {
            return Err(DatabaseError::SystaxError(
                "Expected keyword 'values' or '('",
            ))
        }
    }

    match next_token!(tokens) {
        Token::LeftPren => {
            let mut commas: usize = 0;
            while let Some(value) = tokens.next() {
                match value {
                    Token::Number(value) => {
                        if data.len() != commas {
                            return Err(DatabaseError::SystaxError("Expected an comma"));
                        }
                        data.push(value.to_owned());
                    }
                    Token::String(value) => {
                        if data.len() != commas {
                            return Err(DatabaseError::SystaxError("Expected an comma"));
                        }
                        data.push(value.to_owned());
                    }
                    Token::Comma => commas += 1,
                    Token::RightPren => break,
                    _ => return Err(DatabaseError::SystaxError("Invaild token")),
                }
            }
        }
        _ => return Err(DatabaseError::SystaxError("Expected '('")),
    }

    if !next_token!(tokens).is_token(&Token::SemiComma) {
        return Err(DatabaseError::SystaxError("Expected semicolon"));
    }

    Ok(Statement::Insert {
        table: table_name,
        cols,
        data,
    })
}

fn parse_select(tokens: &mut std::slice::Iter<'_, Token>) -> DBError<Statement> {
    let mut cols = vec![];

    match next_token!(tokens) {
        Token::LeftPren => {
            let mut commas = 0;
            while let Some(value) = tokens.next() {
                match value {
                    Token::Ident(ident) => {
                        if cols.len() != commas {
                            return Err(DatabaseError::SystaxError("Expected an comma"));
                        }

                        cols.push(ident.to_owned());
                    }
                    Token::Comma => {
                        commas += 1;
                    }
                    Token::RightPren => break,
                    _ => {
                        return Err(DatabaseError::SystaxError(
                            "Expected an column name, ')' or ','.",
                        ))
                    }
                }
            }
        }
        Token::Star => {}
        _ => return Err(DatabaseError::SystaxError("Expected '*' or '('")),
    }

    if !next_token!(tokens).is_keyword("from") {
        return Err(DatabaseError::SystaxError("Expected keyword 'from'"));
    }

    let table_name = match next_token!(tokens).get_identifer() {
        Some(i) => i,
        None => return Err(DatabaseError::SystaxError("Invaild token")),
    };

    if !next_token!(tokens).is_token(&Token::SemiComma) {
        return Err(DatabaseError::SystaxError("Expected ';'"));
    }

    Ok(Statement::Select {
        table: table_name,
        columns: cols,
    })
}

mod tests {
    #[test]
    fn test_parse() {
        let input = "insert into TABLE (id,username) values (1,\"tset-user\");".to_string();

        let tokens = super::tokenizer(&input);

        println!("{:#?}", tokens);
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
                    data: vec!["1".to_string(), "test-user".to_string()]
                },
                state
            );
        }
    }
}
